"""Static and pure fail-closed acceptance tests; NOT a substitute for live PostgreSQL."""
import importlib.util
import json
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('lab_operator', ROOT / 'scripts/lab_operator.py')
OP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OP)


def test_compose_namespace_and_gateway_port_isolation():
    doc = yaml.safe_load((ROOT/'compose.b10e5.yaml').read_text())
    assert doc['name'] == 'distributed-banking-b10e5-crash'
    services = doc['services']
    assert set(('gateway','coordinator','shard_a_db','shard_b_db','shard_c_db',
                'bank_a1','bank_a2','bank_b1','bank_b2','bank_c1','bank_c2','operator')) <= services.keys()
    for name, service in services.items():
        if name == 'gateway':
            assert service['ports'] == ['127.0.0.1:${LAB_GATEWAY_PORT:-18082}:8000']
        else:
            assert 'ports' not in service
    assert services['bank_c1']['profiles'] == ['post']
    assert services['operator']['profiles'] == ['ops']
    assert services['operator']['environment']['LAB_ONLY'] == 'distributed-banking-b10e5-crash'


def test_initial_map_and_target_empty():
    baseline = json.loads((ROOT/'config/active_map.json').read_text())
    assert baseline['version'] == 1
    assert len(baseline['shards']) == 2
    target = (ROOT/'database/shard_c_empty.sql').read_text()
    assert 'CREATE TABLE accounts' in target
    assert "generate_series" not in target
    assert 'sandbox-zero-fixture' not in target


def test_operator_endpoints_never_reference_production():
    assert OP.HOSTS == {'A':'shard_a_db', 'B':'shard_b_db', 'C':'shard_c_db'}
    assert OP.DATABASES == {'A':'b10e5_a', 'B':'b10e5_b', 'C':'b10e5_c'}
    assert OP.GATEWAY == 'http://gateway:8000'
    assert OP.COORDINATOR == 'http://coordinator:8000'
    assert OP.EXPECTED_TOTAL == 1999500
    assert not (ROOT/'.env').exists()


@pytest.mark.parametrize('shard', ['', 'D', 'shard_b_db', 'production'])
def test_connection_rejects_unapproved_shard(monkeypatch, shard):
    monkeypatch.setenv('LAB_ONLY', OP.LAB_NAME)
    with pytest.raises(OP.LabRefused, match='Only isolated'):
        OP.connect(shard)


def test_connection_rejects_missing_sentinel(monkeypatch):
    monkeypatch.delenv('LAB_ONLY', raising=False)
    with pytest.raises(OP.LabRefused, match='sentinel'):
        OP.connect('B')


def test_postgres_scripts_require_trigger_fence_and_preserve_staging():
    b = (ROOT/'database/bonus10e2_migration_capture.sql').read_text()
    c = (ROOT/'database/bonus10e4c2_physical_owner_guard.sql').read_text()
    stage = (ROOT/'database/bonus10e4_target_staging.sql').read_text()
    assert "FOR SHARE" in b and "CREATE TRIGGER bank_migration_fence_before" in b
    assert 'physical_shard' in c and 'STALE_OWNER_EPOCH' in c
    assert 'bank_migration_stage_receipts' in stage
    assert 'DROP TABLE accounts' not in c


def test_promote_persists_activation_intent_before_live_changes():
    text = (ROOT/'scripts/lab_operator.py').read_text()
    pos_intent = text.find('j.begin(Step.ACTIVATED)')
    pos_promote = text.find('promote_c(db)', pos_intent)
    pos_clean = text.find('cleanup_b(db)', pos_promote)
    assert 0 <= pos_intent < pos_promote < pos_clean
    assert 'LAB_ONLY' in text
    assert 'zero_prepared(db)' in text


def test_b_cleanup_removes_foreign_key_not_historical_receipts():
    text = (ROOT/'scripts/lab_operator.py').read_text()
    assert 'DROP CONSTRAINT IF EXISTS tpc_participant_commits_account_id_fkey' in text
    assert 'DELETE FROM tpc_participant_commits' not in text
    assert 'ACCESS EXCLUSIVE MODE' in text
    assert 'ALTER TABLE accounts ENABLE TRIGGER bank_migration_fence_before' in text


def test_shell_scripts_do_not_target_original_deploy():
    for f in sorted((ROOT/'scripts').glob('*.sh')):
        s=f.read_text()
        assert 'docker system prune' not in s
        assert 'down -v' not in s
        assert '/opt/distributed-banking-platform' not in s
        assert '127.0.0.1:8001/ready' not in s or 'curl -fsS' in s


def test_all_three_db_credentials_are_isolated():
    doc = yaml.safe_load((ROOT/'compose.b10e5.yaml').read_text())
    services=doc['services']
    for suffix in 'abc':
        db=services[f'shard_{suffix}_db']
        assert db['environment']['POSTGRES_DB']==f'b10e5_{suffix}'
        assert db['volumes']==[f'{suffix}_data:/var/lib/postgresql/data']
        assert db['restart']=='no'
    for bank in 'abc':
        for replica in (1,2):
            service=services[f'bank_{bank}{replica}']
            assert service['environment']['BANK_EPOCH_PROPAGATION']=='1'
            assert service['environment']['BANK_REQUIRE_ROUTING_EPOCH']=='1'
            assert service['environment']['BANK_REQUIRE_SHARD_COHORT']=='1'


def test_operator_autocommit_and_reject_wrong_database(monkeypatch):
    import sys
    import types
    monkeypatch.setenv('LAB_ONLY', OP.LAB_NAME)
    monkeypatch.setenv('LAB_B_PASSWORD','just-for-mock')
    class FakeConn:
        closed=False
        def execute(self,query):
            assert query=='SELECT current_database(), current_user'
            return self
        def fetchone(self):
            return ('b10e5_b','bank')
        def close(self): self.closed=True
    made={}
    def fake_connect(**kwargs):
        made.update(kwargs)
        return FakeConn()
    monkeypatch.setitem(sys.modules,'psycopg',types.SimpleNamespace(connect=fake_connect))
    OP.connect('B')
    assert made['autocommit'] is True
    assert made['host']=='shard_b_db'
    assert made['dbname']=='b10e5_b'
    assert made['connect_timeout']==4


def test_reject_fake_real_db_response(monkeypatch):
    import sys
    import types
    monkeypatch.setenv('LAB_ONLY', OP.LAB_NAME)
    monkeypatch.setenv('LAB_A_PASSWORD','mock-only')
    class FakeConn:
        closed=False
        def execute(self, _): return self
        def fetchone(self): return ('bank_a','bank')
        def close(self): self.closed=True
    fake=FakeConn()
    monkeypatch.setitem(sys.modules,'psycopg',types.SimpleNamespace(connect=lambda **kw:fake))
    with pytest.raises(OP.LabRefused, match='DB identity'):
        OP.connect('A')
    assert fake.closed


def test_script_cutover_safety():
    text=(ROOT/'scripts/publish_lab.sh').read_text()
    assert 'bash scripts/operator.sh prepublish' in text
    assert 'shard_map_3shard.example.json' in text
    assert "q.replace(p)" in text
    assert "LAB_ROUTING_EPOCH=2" in text
    assert 'bash scripts/operator.sh verify-live' in text
    assert 'bash scripts/operator.sh post-2pc' not in text  # B10E5 runs 2PC once, separately after durable verify


def test_bootstrap_scripts_do_not_reseed_after_shrink():
    b=(ROOT/'scripts/init_b.sh').read_text()
    c=(ROOT/'scripts/init_c.sh').read_text()
    assert 'hi=1500 AND n=500' in b
    assert 'hi=2000 AND n=1000' in b
    assert 'n=0' in c and 'n=500' in c
    assert 'EXISTING_DB_VERIFY_ONLY' in b
    assert 'EXISTING_DB_VERIFY_ONLY' in c


def test_negative_sql_guards_run_before_activation():
    text=(ROOT/'scripts/lab_operator.py').read_text()
    pos_c=text.find('guard_rejection_tests(db, after_source_fence=False)')
    pos_snap=text.find('j.begin(Step.SNAPSHOTTING)')
    pos_b=text.find('guard_rejection_tests(db, after_source_fence=True)')
    pos_conv=text.find('stage.fenced_convergence()')
    assert 0 < pos_c < pos_snap
    assert 0 < pos_b < pos_conv
    assert 'except psycopg.Error as exc:' in text


def test_no_sensitive_files_in_lab_tree():
    assert not (ROOT/'.env').exists()
    assert not list(ROOT.rglob('*.pem'))
    assert not list(ROOT.rglob('*.key'))
    assert (ROOT/'.gitignore').is_file()
