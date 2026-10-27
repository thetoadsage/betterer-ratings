import copy
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from scripts.repair_anime_mappings import apply, load_plan, preview
from tests.test_mal_enrichment import MOVIE


class FakeAPI:
    def __init__(self, entries=None, account='worker'):
        self.entries = copy.deepcopy(entries or [])
        self.calls = []
        self.account = account
        self.fail_post = False
        self.confirm_new = True

    def request(self, method, path, params=None, body=None):
        self.calls.append((method, path, params, body))
        if method == 'GET' and path.endswith('/lookup'):
            rows = sorted({(e['media_type'],e['tmdb_id']) for e in self.entries if e['value']==params['id_value'] and e['media_type']==params['media_type']})
            return {'results':[{'media_type':kind,'tmdb_id':i} for kind,i in rows], 'total':len(rows)}
        if method == 'GET':
            rows = [dict(e) for e in self.entries if e['tmdb_id']==params['tmdb_id'] and e['media_type']==params['media_type']]
            return {'tmdb_id':params['tmdb_id'],'media_type':params['media_type'],'mappings':{'mal':rows},'total':len(rows)}
        if method == 'DELETE':
            old = next(e for e in self.entries if e['id']==path.rsplit('/',1)[1])
            if old['contributor'] != self.account:
                raise RuntimeError('HTTP 403')
            self.entries.remove(old)
            return {'success':True}
        if self.fail_post:
            raise RuntimeError('HTTP 429; stop')
        assert body['id_type']=='mal'
        assert not any(e['value']==body['id_value'] and e['media_type']==body['media_type'] for e in self.entries)
        e = entry(body['tmdb_id'],body['id_value'],self.account,'created-'+str(body['tmdb_id']))
        if self.confirm_new:
            self.entries.append(e)
        return {'success':True,'item':{'id':e['id']}}


def entry(tmdb=128, value='999', contributor='worker', id='old'):
    return {'tmdb_id':tmdb,'media_type':'movie','value':value,'id':id,'contributor':contributor}


def setup(db, tmdb=128, old='999', new='164'):
    db.conn.execute("INSERT INTO mappings(tmdb_id,media_type,id_type,id_value,fetched_at,pmdb_status,pmdb_item_id) VALUES (?,'movie','mal',?,1,'submitted','stale-cached-id')", (tmdb,old))
    db.conn.execute("INSERT INTO ratings(tmdb_id,media_type,label,score,fetched_at,pmdb_status,pmdb_item_id) VALUES (?,'movie','ML',60,1,'submitted','rating-keep')", (tmdb,))
    db.conn.execute("INSERT INTO anime_mapping_holds VALUES (?,'movie','mal','mapping_conflict',1)",(tmdb,))
    db.conn.commit()
    db.save_enrichment_state(tmdb,'movie','tmdb',payload=MOVIE,now_ts=1,next_due=100)
    db.save_enrichment_state(tmdb,'movie','mal',payload={'old':True},now_ts=1,next_due=100)
    return {'title':'Princess Mononoke','tmdb_id':tmdb,'media_type':'movie','old_mal_id':old,'new_mal_id':new,'tmdb_details':MOVIE,
        'review_decision':'replacement_identity_verified','review_note':'Reviewed official metadata',
        'checked_at':datetime.now(timezone.utc).isoformat()}


def run_apply(db, api, report, tmp_path, **kwargs):
    output=tmp_path/'run'
    output.mkdir(exist_ok=True)
    return apply(db.path,api,report,output,service_stopped=True,**kwargs)


def test_preview_has_no_writes_and_correct_existing_entry_needs_only_cleanup(local_db):
    row=setup(local_db)
    api=FakeAPI([entry(),entry(value='164',contributor='someone-else',id='correct')])
    before=hashlib.sha256(local_db.path.read_bytes()).hexdigest()
    report=preview(local_db.conn,api,[row],'worker')
    assert report['delete_count']==1 and report['create_count']==0
    assert all(method=='GET' for method,*_ in api.calls)
    assert hashlib.sha256(local_db.path.read_bytes()).hexdigest()==before


def test_delete_only_our_bad_mapping_and_preserve_rating_history(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry(),entry(value='164',contributor='other',id='correct'),entry(tmdb=129,value='999',id='unrelated')])
    report=preview(local_db.conn,api,[row],'worker')
    ratings=local_db.conn.execute('SELECT * FROM ratings').fetchall()
    result=run_apply(local_db,api,report,tmp_path)
    assert result['updated_local']==1
    assert not any(c[0]=='POST' for c in api.calls)
    assert {e['id'] for e in api.entries}=={'correct','unrelated'}
    mapping=local_db.conn.execute('SELECT * FROM mappings').fetchone()
    assert mapping['id_value']=='164' and mapping['pmdb_status']=='submitted' and mapping['pmdb_item_id'] is None
    assert local_db.conn.execute('SELECT * FROM ratings').fetchall()==ratings
    assert local_db.conn.execute('SELECT reason FROM anime_mapping_holds').fetchone()[0]=='mapping_conflict'
    with sqlite3.connect(result['backup']) as c:
        assert c.execute('SELECT id_value,pmdb_item_id FROM mappings').fetchone()==('999','stale-cached-id')
    assert local_db.conn.execute("SELECT payload,next_due FROM enrichment_state WHERE provider='mal'").fetchone()[:]==(None,0)


def test_missing_correct_mapping_is_created_and_owned_id_is_cached(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()])
    report=preview(local_db.conn,api,[row],'worker')
    run_apply(local_db,api,report,tmp_path)
    assert local_db.conn.execute('SELECT id_value,pmdb_item_id FROM mappings').fetchone()[:]==('164','created-128')
    assert sum(c[0]=='POST' for c in api.calls)==1
    # Repeating the complete repair performs no DELETE or POST.
    api.calls.clear()
    report=preview(local_db.conn,api,[row],'worker')
    assert report['delete_count']==report['create_count']==0
    run_apply(local_db,api,report,tmp_path)
    assert all(c[0]=='GET' for c in api.calls)


def test_batch_swap_deletes_both_old_entries_before_creating_either(local_db,tmp_path):
    rows=[setup(local_db,128,'1','2'),setup(local_db,129,'2','1')]
    api=FakeAPI([entry(128,'1',id='a'),entry(129,'2',id='b')])
    report=preview(local_db.conn,api,rows,'worker')
    assert len(report['ready'])==2
    run_apply(local_db,api,report,tmp_path)
    mutations=[c[0] for c in api.calls if c[0]!='GET']
    assert mutations==['DELETE','DELETE','POST','POST']
    assert [r[:] for r in local_db.conn.execute('SELECT tmdb_id,id_value FROM mappings ORDER BY tmdb_id')]==[(128,'2'),(129,'1')]


def test_foreign_wrong_entry_is_never_deleted(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry(contributor='other')])
    report=preview(local_db.conn,api,[row],'worker')
    assert len(report['blocked'])==1
    with pytest.raises(ValueError,match='blocked'):
        run_apply(local_db,api,report,tmp_path)
    assert all(c[0]=='GET' for c in api.calls)


def test_unresolved_local_owner_blocks_even_when_remote_correct_exists(local_db):
    row=setup(local_db)
    setup(local_db,129,'164','165')
    api=FakeAPI([entry(),entry(value='164',id='correct')])
    report=preview(local_db.conn,api,[row],'worker')
    assert len(report['blocked'])==1


def test_remote_other_owner_blocks_create(local_db):
    row=setup(local_db)
    api=FakeAPI([entry(),entry(129,'164',id='owner')])
    assert len(preview(local_db.conn,api,[row],'worker')['blocked'])==1


def test_changed_local_metadata_or_value_blocks(local_db):
    row=setup(local_db)
    local_db.save_enrichment_state(128,'movie','tmdb',payload={**MOVIE,'release_date':'2000-01-01'},now_ts=1,next_due=2)
    api=FakeAPI([entry()])
    assert len(preview(local_db.conn,api,[row],'worker')['blocked'])==1
    assert api.calls==[]


def test_forward_reverse_disagreement_blocks_before_delete(local_db):
    row=setup(local_db)
    class Inconsistent(FakeAPI):
        def request(self,method,path,params=None,body=None):
            if path.endswith('/lookup') and params['id_value']=='999':
                return {'results':[],'total':0}
            return super().request(method,path,params,body)
    api=Inconsistent([entry()])
    assert len(preview(local_db.conn,api,[row],'worker')['blocked'])==1


def test_wrong_api_account_stops_on_forbidden_without_local_update(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()],account='different-account')
    report=preview(local_db.conn,api,[row],'worker')
    with pytest.raises(RuntimeError,match='403'):
        run_apply(local_db,api,report,tmp_path)
    assert local_db.get_title_mapping(tmdb_id=128,media_type='movie',id_type='mal')=='999'
    assert len(api.entries)==1


def test_interrupted_remote_create_can_resume_without_redeleting(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()])
    report=preview(local_db.conn,api,[row],'worker')
    api.fail_post=True
    with pytest.raises(RuntimeError,match='429'):
        run_apply(local_db,api,report,tmp_path)
    assert not api.entries
    assert local_db.get_title_mapping(tmdb_id=128,media_type='movie',id_type='mal')=='999'
    api.fail_post=False
    report=preview(local_db.conn,api,[row],'worker')
    assert report['delete_count']==0 and report['create_count']==1
    run_apply(local_db,api,report,tmp_path)
    assert local_db.get_title_mapping(tmdb_id=128,media_type='movie',id_type='mal')=='164'


def test_stale_deletion_target_is_rechecked_before_any_delete(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()])
    report=preview(local_db.conn,api,[row],'worker')
    api.entries[0]['contributor']='someone-else'
    with pytest.raises(ValueError,match='deletion target changed'):
        run_apply(local_db,api,report,tmp_path)
    assert all(c[0]=='GET' for c in api.calls)


def test_unconfirmed_create_does_not_update_local_db(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()])
    report=preview(local_db.conn,api,[row],'worker')
    api.confirm_new=False
    with pytest.raises(ValueError,match='not confirmed'):
        run_apply(local_db,api,report,tmp_path)
    assert local_db.get_title_mapping(tmdb_id=128,media_type='movie',id_type='mal')=='999'


def test_apply_requires_stopped_service_before_mutations(local_db,tmp_path):
    row=setup(local_db)
    api=FakeAPI([entry()])
    report=preview(local_db.conn,api,[row],'worker')
    with pytest.raises(ValueError,match='Stop Betterer'):
        apply(local_db.path,api,report,tmp_path,service_stopped=False)
    assert all(c[0]=='GET' for c in api.calls)


@pytest.mark.parametrize('change',['stale','duplicate','invalid_id','unreviewed'])
def test_bad_plan_is_refused_before_network(tmp_path,change):
    row={'tmdb_id':1,'media_type':'movie','old_mal_id':'1','new_mal_id':'2','review_decision':'replacement_identity_verified','review_note':'reviewed','checked_at':datetime.now(timezone.utc).isoformat()}
    rows=[row]
    if change=='stale':
        row['checked_at']='2000-01-01T00:00:00+00:00'
    elif change=='duplicate':
        rows.append(dict(row))
    elif change=='invalid_id':
        row['new_mal_id']='-2'
    else:
        row['review_decision']='candidate_not_fully_verified'
    path=tmp_path/'plan.json'
    path.write_text(json.dumps({'version':1,'corrections':rows}))
    with pytest.raises(ValueError):
        load_plan(path)


@pytest.mark.parametrize('payload,empty', [({'error':'No matching TMDB ID found'},True),({'error':'Route not found'},False)])
def test_only_exact_reverse_no_owner_404_is_treated_as_empty(monkeypatch,payload,empty):
    import io
    import urllib.error

    from scripts.repair_anime_mappings import API

    api=API('https://example.test','secret')
    def missing(req,timeout):
        raise urllib.error.HTTPError(req.full_url,404,'not found',{},io.BytesIO(json.dumps(payload).encode()))
    monkeypatch.setattr(api.opener,'open',missing)
    if empty:
        assert api.request('GET','/api/external/mappings/lookup',{'id_type':'mal','id_value':'1'})=={'results':[],'total':0}
    else:
        with pytest.raises(RuntimeError,match='404'):
            api.request('GET','/api/external/mappings/lookup',{'id_type':'mal','id_value':'1'})


def test_partial_batch_requires_explicit_selection(local_db,tmp_path):
    a=setup(local_db,128,'1','2')
    b=setup(local_db,129,'3','4')
    api=FakeAPI([entry(128,'1',id='a'),entry(129,'3',contributor='other',id='b')])
    report=preview(local_db.conn,api,[a,b],'worker')
    assert len(report['ready'])==len(report['blocked'])==1
    with pytest.raises(ValueError,match='blocked'):
        run_apply(local_db,api,report,tmp_path)
    result=run_apply(local_db,api,report,tmp_path,only_ready=True)
    assert result['updated_local']==1
    assert local_db.get_title_mapping(tmdb_id=129,media_type='movie',id_type='mal')=='3'
    assert any(e['id']=='b' for e in api.entries)


def test_journal_never_records_a_token_even_if_provider_echoes_it():
    from scripts.repair_anime_mappings import API
    events=[]
    api=API('https://example.test','private-api-key',journal=events.append)
    api.record({'payload':{'message':'private-api-key'}})
    assert events==[{'payload':{'message':'[REDACTED]'}}]
