import importlib.util
import subprocess
from pathlib import Path
import pytest
import yaml
ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('op', ROOT/'scripts/lab_operator.py')
OP=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(OP)

def test_isolation():
    doc=yaml.safe_load((ROOT/'compose.b10e5.yaml').read_text())
    assert doc['name']=='distributed-banking-b10e5-crash'
    assert doc['services']['gateway']['ports']==['127.0.0.1:${LAB_GATEWAY_PORT:-18082}:8000']
    assert doc['services']['operator']['environment']['PYTHONPATH']=='/app'
    assert doc['services']['operator']['environment']['LAB_ONLY']=='distributed-banking-b10e5-crash'
    assert OP.DATABASES=={'A':'b10e5_a','B':'b10e5_b','C':'b10e5_c'}
    assert not (ROOT/'.env').exists()

def test_allowlist():
    assert OP.INJECTION_POINTS=={'CAPTURE_AFTER_COMMIT','SNAPSHOT_AFTER_COMMIT','FENCE_AFTER_COMMIT','PROMOTE_AFTER_C_ACTIVATION','PROMOTE_AFTER_B_CLEANUP'}

def test_failpoints_bounded_and_ordered():
    src=(ROOT/'scripts/lab_operator.py').read_text()
    for point,before,after in [('CAPTURE_AFTER_COMMIT','with b.transaction():','confirm_step(j, Step.SNAPSHOTTING, db)'),('SNAPSHOT_AFTER_COMMIT',"result = PostgreSQLStaging(db['B'], db['C']).snapshot()",'confirm_step(j, Step.CATCHING_UP, db)'),('FENCE_AFTER_COMMIT','j.begin(Step.FENCED)','guard_rejection_tests(db, after_source_fence=True)'),('PROMOTE_AFTER_C_ACTIVATION','promote_c(db)','cleanup_b(db)')]:
        k=src.index("crash_checkpoint('"+point+"')")
        assert src.rfind(before,0,k)>=0 and src.find(after,k)>k
    assert 'if staged == 0:' in src
    assert "if fence(db['B']) == ('B',1,False,True):" in src


def test_publish_not_duplicate_financial_mutation():
    s=(ROOT/'scripts/publish_lab.sh').read_text()
    assert 'PUBLISH_AFTER_MAP' in s
    assert 'LAB_MAP_ALREADY_VERSION2' in s
    assert 'bash scripts/operator.sh post-2pc' not in s
    assert 'B10E5_GATEWAY_NOT_READY_AFTER_PUBLISH' in s


def test_safe_bootstrap_and_scripts():
    assert '/bootstrap/shard.sql' in (ROOT/'scripts/init_b.sh').read_text()
    assert '/bootstrap/shard_b.sql' not in (ROOT/'scripts/init_b.sh').read_text()
    for p in (ROOT/'scripts').glob('*.sh'):
        s=p.read_text(); assert 'down -v' not in s and 'docker system prune' not in s
    s=(ROOT/'scripts/crash_matrix.sh').read_text()
    assert s.count('bash scripts/operator.sh exercise')==1
    assert 'UNEXPECTED_OPERATOR_EXIT' in s and 'BONUS10E5_INTEGRATED_CRASH_RESUME_MATRIX_PASS' in s


def test_real_abrupt_exit():
    script = "import os,sys; sys.path.insert(0,sys.argv[1]); import lab_operator as o; os.environ['LAB_ONLY']=o.LAB_NAME; os.environ['B10E5_FAULT_POINT']='CAPTURE_AFTER_COMMIT'; o.crash_checkpoint('CAPTURE_AFTER_COMMIT')"
    # test subprocess, not the runner process
    p=subprocess.run(['python3','-c',script,str(ROOT/'scripts')],capture_output=True,text=True)
    assert p.returncode==86
    assert 'BONUS10E5_EXPECTED_CRASH: CAPTURE_AFTER_COMMIT' in p.stdout


def test_nonmatching_fault(monkeypatch):
    monkeypatch.setenv('LAB_ONLY',OP.LAB_NAME)
    monkeypatch.setenv('B10E5_FAULT_POINT','SNAPSHOT_AFTER_COMMIT')
    OP.crash_checkpoint('CAPTURE_AFTER_COMMIT')


def test_matrix_requires_fresh_journal_proof():
    s=(ROOT/'scripts/crash_matrix.sh').read_text()
    assert 'python3 scripts/check_fresh.py' in s
    check=(ROOT/'scripts/check_fresh.py').read_text()
    assert "j['phase']=='PLANNED'" in check
    assert "j['transition_count']==0" in check
    assert 'B10E5_NOT_FRESH_DO_NOT_REPEAT_MATRIX' in check
