"""Unit tests for federation_state.

Covers: the record dataclasses and their wire/persist views, the durable
``state.json`` round-trip, the submit-ledger counters, node-hour arithmetic
over a dispatcher ``pilot_history``, and the join-time validators.
"""

import json
import os
import time

from pathlib import Path

import pytest

from radical.orbit.federation_state import (
    FederationState, FederationStateError, MemberRecord, ResourceRecord,
    ResourceUsage, SubmitLedgerEntry,
    LIVENESS_LOST, LIVENESS_SUSPECT, MODE_ALLOCATION, MODE_LOGIN,
    allowed_bases, member_from_dict,
    record_from_dict, ledger_from_dict, resource_attributes,
    validate_attributes, validate_budget,
    validate_capabilities, validate_class, validate_member_name,
    validate_name, validate_pool_int, validate_scratch_base,
    validate_scratch_for_host, validate_software,
)


def _rec(**overrides) -> ResourceRecord:
    defaults = dict(
        name='alpha', endpoint='ep0', site='HERE', kind='workstation',
        capabilities={'cores': 8, 'gpus': 0, 'software': ['lammps']},
        budget={'node_hours': 4.0},
        scratch_base='/tmp/fed/alpha',
        joined_at=1000.0, dispatcher_sid='fed-alpha', pool_name='fed-alpha',
    )
    defaults.update(overrides)
    return ResourceRecord(**defaults)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

class TestResourceRecord:

    def test_budget_node_hours(self):
        assert _rec().budget_node_hours() == 4.0
        assert _rec(budget={}).budget_node_hours() == 0.0
        assert _rec(budget={'node_hours': 'nope'}).budget_node_hours() == 0.0

    def test_capability_lookup(self):
        r = _rec()
        assert r.capability('cores') == 8
        assert r.capability('nope') is None
        assert r.capability('nope', 3) == 3

    def test_to_wire_hides_the_internal_pool_config(self):
        r = _rec(pool_config={'name': 'fed-alpha', 'queue': 'allocation'})
        assert 'pool_config' in r.to_dict()
        assert 'pool_config' not in r.to_wire()
        # everything a client is promised is still there
        for key in ('name', 'endpoint', 'mode', 'site', 'kind',
                    'capabilities', 'budget', 'scratch_base', 'joined_at',
                    'dispatcher_sid', 'pool_name', 'usage', 'liveness'):
            assert key in r.to_wire()

    def test_record_from_dict_rebuilds_usage_and_drops_unknowns(self):
        r = record_from_dict({
            'name': 'a', 'endpoint': 'e', 'future_field': 1,
            'usage': {'node_hours_used': 2.5, 'bogus': 9},
        })
        assert isinstance(r.usage, ResourceUsage)
        assert r.usage.node_hours_used == 2.5
        assert not hasattr(r, 'future_field')

    def test_record_from_dict_without_usage(self):
        r = record_from_dict({'name': 'a', 'endpoint': 'e'})
        assert r.usage.node_hours_used == 0.0

    def test_ledger_from_dict_drops_unknowns(self):
        e = ledger_from_dict({'task_id': 't', 'resource': 'a', 'junk': 1})
        assert isinstance(e, SubmitLedgerEntry)
        assert e.task_id == 't' and e.state == 'QUEUED'


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------

class TestFederationState:

    def test_load_missing_file_is_empty(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json').load()
        assert st.resources == {} and st.ledger == {}

    def test_load_malformed_file_is_empty(self, tmp_path: Path):
        p = tmp_path / 'state.json'
        p.write_text('{not json')
        st = FederationState(p).load()
        assert st.resources == {}

    def test_round_trip(self, tmp_path: Path):
        p  = tmp_path / 'sub' / 'state.json'
        st = FederationState(p)
        rec = _rec(pool_config={'name': 'fed-alpha', 'queue': 'allocation'})
        rec.usage.node_hours_used = 1.25
        st.resources['alpha'] = rec
        st.ledger['t.1'] = SubmitLedgerEntry(
            task_id='t.1', resource='alpha', pool='fed-alpha',
            dispatcher_sid='fed-alpha', state='DONE', submitted_at=5.0)
        st.save()

        back = FederationState(p).load()
        assert set(back.resources) == {'alpha'}
        got = back.resources['alpha']
        assert got.capabilities == {'cores': 8, 'gpus': 0,
                                    'software': ['lammps']}
        assert got.usage.node_hours_used == 1.25
        # the resolved pool declaration survives — a restart must be able to
        # re-register the *identical* pool
        assert got.pool_config['queue'] == 'allocation'
        assert back.ledger['t.1'].state == 'DONE'

    def test_round_trip_keeps_an_unshared_scratch(self, tmp_path: Path):
        p  = tmp_path / 'state.json'
        st = FederationState(p)
        st.resources['alpha'] = _rec(
            shared_fs=False, scratch_base='/pscratch/sd/m/x/demo',
            members={'default': MemberRecord(
                member='default', shared_fs=False,
                scratch_base='/pscratch/sd/m/x/demo')})
        st.save()
        got = FederationState(p).load().resources['alpha']
        assert got.shared_fs   is False
        assert got.scratch_base == '/pscratch/sd/m/x/demo'
        # ... and so does its member's
        assert got.members['default'].shared_fs is False

    def test_a_state_file_without_shared_fs_loads_as_shared(self,
                                                            tmp_path: Path):
        # every record written before the flag existed describes a resource
        # on the broker's own filesystem
        p = tmp_path / 'state.json'
        p.write_text(json.dumps({'resources': {'alpha': {
            'name': 'alpha', 'endpoint': 'ep0',
            'scratch_base': '/tmp/fed/alpha'}}}))
        got = FederationState(p).load().resources['alpha']
        assert got.shared_fs is True
        assert got.members['default'].shared_fs is True

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        st = FederationState(tmp_path / 'a' / 'b' / 'state.json')
        st.save()
        assert (tmp_path / 'a' / 'b' / 'state.json').is_file()

    def test_task_counts_split_by_state(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json')
        for tid, res, state in (('t.1', 'a', 'QUEUED'),
                                ('t.2', 'a', 'RUNNING'),
                                ('t.3', 'a', 'DONE'),
                                ('t.4', 'a', 'FAILED'),
                                ('t.5', 'a', 'CANCELED'),
                                ('t.6', 'b', 'DONE')):
            st.ledger[tid] = SubmitLedgerEntry(task_id=tid, resource=res,
                                               state=state)
        # QUEUED counts as running: from the federation's point of view a
        # task waiting on a warming pilot is still work in flight.
        assert st.task_counts('a') == (2, 1, 2)
        assert st.task_counts('b') == (0, 1, 0)
        assert st.task_counts('nope') == (0, 0, 0)

    def test_drop_resource_also_drops_its_ledger(self, tmp_path: Path):
        st = FederationState(tmp_path / 'state.json')
        st.resources['a'] = _rec(name='a')
        st.ledger['t.1'] = SubmitLedgerEntry(task_id='t.1', resource='a')
        st.ledger['t.2'] = SubmitLedgerEntry(task_id='t.2', resource='b')
        st.drop_resource('a')
        assert 'a' not in st.resources
        assert set(st.ledger) == {'t.2'}


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

class TestValidators:

    def test_name_pattern(self):
        assert validate_name('perlmutter_a.1-x') == 'perlmutter_a.1-x'
        for bad in ('', None, 'Upper', 'has space', 'slash/es', 42):
            with pytest.raises(FederationStateError):
                validate_name(bad)

    def test_capabilities_pass_through_numbers_and_software(self):
        caps = validate_capabilities({'cores': 8, 'mem_gb': 12.5,
                                      'software': ['a', 'b']})
        assert caps == {'cores': 8, 'mem_gb': 12.5, 'software': ['a', 'b']}

    def test_capabilities_none_is_empty(self):
        assert validate_capabilities(None) == {}

    def test_capabilities_reject_bad_shapes(self):
        for bad in ([], {'cores': 'many'}, {'cores': True},
                    {'software': 'lammps'}, {'software': [1]}, {'': 1}):
            with pytest.raises(FederationStateError):
                validate_capabilities(bad)

    def test_budget_optional_by_default(self):
        assert validate_budget(None) == {}
        assert validate_budget({}) == {}

    def test_budget_required_in_login_mode(self):
        with pytest.raises(FederationStateError, match='node_hours'):
            validate_budget({}, required=True)

    def test_budget_must_be_positive(self):
        assert validate_budget({'node_hours': 4}) == {'node_hours': 4.0}
        for bad in ({'node_hours': 0}, {'node_hours': -1},
                    {'node_hours': True}, {'node_hours': 'x'}, []):
            with pytest.raises(FederationStateError):
                validate_budget(bad)

    def test_scratch_base_under_tmp_or_home(self, tmp_path):
        assert validate_scratch_base('/tmp/fed/x') == '/tmp/fed/x'
        home = str(Path.home() / '.radical' / 'orbit' / 'fed')
        assert validate_scratch_base(home) == home
        assert validate_scratch_base('~/fed-x') == str(Path.home() / 'fed-x')

    def test_scratch_base_elsewhere_is_rejected(self):
        for bad in ('/etc/passwd', '/var/lib/x', 'relative/path',
                    '/tmp/../etc', '', None):
            with pytest.raises(FederationStateError):
                validate_scratch_base(bad)

    def test_scratch_base_realpath_catches_a_symlink_escape(self):
        # The check is on the realpath (as plugin_staging does), so a link
        # under an allowed root cannot aim the tree out of it.
        link = Path('/tmp') / f'orbit-fed-link-{os.getpid()}'
        link.unlink(missing_ok=True)
        os.symlink('/etc', link)
        try:
            with pytest.raises(FederationStateError, match='resolves to'):
                validate_scratch_base(str(link))
        finally:
            link.unlink(missing_ok=True)

    def test_scratch_base_returns_the_declared_form(self):
        # Validation resolves; the value kept is what the operator wrote.
        assert validate_scratch_base('~/fed-x') == str(Path.home() / 'fed-x')

    def test_scratch_base_names_the_field_it_rejected(self):
        with pytest.raises(FederationStateError, match='task.cwd'):
            validate_scratch_base('/etc/x', field='task.cwd')

    def test_a_shared_scratch_goes_through_the_containment_rule(self):
        assert validate_scratch_for_host('/tmp/fed/x') == '/tmp/fed/x'
        with pytest.raises(FederationStateError, match='must lie under'):
            validate_scratch_for_host('/pscratch/sd/m/x', shared=True)

    def test_an_unshared_scratch_is_kept_exactly_as_declared(self):
        # the path lives on another host: neither expanded nor resolved here
        for path in ('/pscratch/sd/m/x/atomic-demo', '~/atomic-demo',
                     '/ccsopen/home/x/tmp/atomic-demo/odo'):
            assert validate_scratch_for_host(path, shared=False) == path

    def test_an_unshared_scratch_must_still_be_absolute(self):
        for bad in ('relative/path', '', None, 3):
            with pytest.raises(FederationStateError):
                validate_scratch_for_host(bad, shared=False)

    def test_an_unshared_scratch_names_the_field_it_rejected(self):
        with pytest.raises(FederationStateError, match='member gpu'):
            validate_scratch_for_host('rel', shared=False,
                                      field='member gpu.scratch_base')

    def test_allowed_bases_are_realpaths(self):
        assert allowed_bases() == [os.path.realpath(os.path.expanduser('~')),
                                   os.path.realpath('/tmp')]


class TestValidatePoolInt:

    def test_returns_the_value(self):
        assert validate_pool_int({'nodes': 4}, 'nodes', minimum=1) == 4

    def test_missing_with_a_default(self):
        assert validate_pool_int({}, 'max_pilots', default=1, minimum=1) == 1

    def test_explicit_null_falls_back_to_the_default(self):
        assert validate_pool_int({'gpus_per_node': None}, 'gpus_per_node',
                                 default=0, minimum=0) == 0

    def test_missing_without_a_default_is_required(self):
        with pytest.raises(FederationStateError, match='required'):
            validate_pool_int({}, 'nodes', minimum=1)

    def test_non_integers_are_rejected(self):
        for bad in ('two', 2.5, [2], True):
            with pytest.raises(FederationStateError):
                validate_pool_int({'nodes': bad}, 'nodes', minimum=1)

    def test_range_is_enforced_at_both_ends(self):
        with pytest.raises(FederationStateError, match='>= 1'):
            validate_pool_int({'nodes': 0}, 'nodes', minimum=1)
        with pytest.raises(FederationStateError, match='<= 10'):
            validate_pool_int({'nodes': 11}, 'nodes', minimum=1, maximum=10)

    def test_the_message_names_the_pool_field(self):
        with pytest.raises(FederationStateError, match="'pool.max_pilots'"):
            validate_pool_int({'max_pilots': 'two'}, 'max_pilots', minimum=1)


class TestModes:

    def test_a_login_record_keeps_its_mode_through_persistence(self,
                                                               tmp_path):
        p  = tmp_path / 'state.json'
        st = FederationState(p)
        st.resources['b'] = _rec(name='b', mode=MODE_LOGIN,
                                 pool={'queue': 'regular', 'nodes': 2})
        st.save()
        back = FederationState(p).load().resources['b']
        assert back.mode == MODE_LOGIN
        assert back.pool == {'queue': 'regular', 'nodes': 2}

    def test_a_persisted_liveness_is_restored_verbatim(self, tmp_path):
        # The plugin overrides this to LOST at load — nothing has seen a
        # participant yet — but the store itself must not second-guess the
        # file it was given.
        p  = tmp_path / 'state.json'
        st = FederationState(p)
        st.resources['a'] = _rec(liveness=LIVENESS_LOST)
        st.save()
        assert FederationState(p).load().resources['a'].liveness == \
            LIVENESS_LOST


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------

def _member(**overrides) -> MemberRecord:
    defaults = dict(member='gpu', member_id='beta.gpu', cls='gpu',
                    pool_name='fed-gpu', queue='GPU', account='m1234',
                    nodes=1, cpus_per_node=64, gpus_per_node=8,
                    walltime_sec=1800, min_pilots=0, max_pilots=2,
                    software=['pytorch'],
                    attributes={'site': 'PSC', 'mem_gb_per_node': 256},
                    budget={'node_hours': 8.0})
    defaults.update(overrides)
    return MemberRecord(**defaults)


class TestMemberRecord:

    def test_to_wire_renames_cls_to_class(self):
        wire = _member().to_wire()
        assert wire['class'] == 'gpu'
        assert 'cls' not in wire
        # everything the CLI and the Explorer read is there
        for key in ('member', 'member_id', 'pool_name', 'queue', 'nodes',
                    'cpus_per_node', 'gpus_per_node', 'software',
                    'attributes', 'budget', 'usage', 'liveness'):
            assert key in wire

    def test_member_from_dict_accepts_both_spellings(self):
        assert member_from_dict({'member': 'a', 'class': 'gpu'}).cls == 'gpu'
        assert member_from_dict({'member': 'a', 'cls': 'gpu'}).cls   == 'gpu'

    def test_member_round_trip_through_the_wire(self):
        back = member_from_dict(_member().to_wire())
        assert back == _member()

    def test_member_from_dict_drops_unknowns_and_rebuilds_usage(self):
        m = member_from_dict({'member': 'a', 'future': 1,
                              'usage': {'pilots_active': 2, 'bogus': 9}})
        assert isinstance(m.usage, ResourceUsage)
        assert m.usage.pilots_active == 2
        assert not hasattr(m, 'future')

    def test_default_class_follows_the_declared_gpus(self):
        assert _member(gpus_per_node=0).default_class() == 'cpu'
        assert _member(gpus_per_node=1).default_class() == 'gpu'

    def test_pilot_size_is_the_shape_the_matcher_compares(self):
        size = _member().pilot_size()
        assert (size.nodes, size.cpus_per_node, size.gpus_per_node) == \
            (1, 64, 8)

    def test_match_attributes_folds_software_in(self):
        attrs = _member().match_attributes()
        assert attrs['software'] == ['pytorch']
        assert attrs['site']     == 'PSC'
        # and does not mutate the member
        assert 'software' not in _member().attributes

    def test_budget_node_hours(self):
        assert _member().budget_node_hours() == 8.0
        assert _member(budget={}).budget_node_hours() == 0.0


class TestDerivedState:
    """``state()``: the word a human is shown, beside the raw liveness."""

    def _usage(self, **kw):
        return ResourceUsage(**kw)

    def test_a_member_holding_a_pilot_is_ok(self):
        m = _member(usage=self._usage(pilots_active=1))
        assert m.state() == 'ok'

    def test_a_member_with_no_pilot_and_no_failure_is_idle(self):
        """The join-to-first-pilot window, and a login shape at rest.  It
        must never read `failing` merely because no pilot is up yet."""
        assert _member().state() == 'idle'

    def test_a_paused_member_is_failing(self):
        m = _member(usage=self._usage(paused_until=time.time() + 60,
                                      pilot_error='psij error: quota'))
        assert m.state() == 'failing'

    def test_a_repeatedly_failing_member_is_failing(self):
        m = _member(usage=self._usage(pilot_failures=3,
                                      pilot_error='psij error: quota'))
        assert m.state() == 'failing'

    def test_one_recorded_failure_is_neither_idle_nor_failing(self):
        m = _member(usage=self._usage(pilot_failures=1,
                                      pilot_error='psij error: quota'))
        assert m.state() == 'ok'

    def test_a_stale_refresh_says_so(self):
        assert _member(usage=self._usage(stale=True)).state() == 'stale'

    def test_liveness_passes_through_untouched(self):
        assert _member(liveness=LIVENESS_LOST).state()    == 'lost'
        assert _member(liveness=LIVENESS_SUSPECT).state() == 'suspect'

    def test_a_resource_shows_the_worst_of_its_shapes(self):
        rec = _rec(name='beta')
        rec.members['cpu'] = _member(member='cpu',
                                     usage=self._usage(pilots_active=1))
        rec.members['gpu'] = _member(
            member='gpu', usage=self._usage(pilot_failures=4,
                                            pilot_error='psij error: quota'))
        assert rec.state() == 'failing'

    def test_one_resting_shape_does_not_make_the_machine_idle(self):
        rec = _rec(name='beta')
        rec.members['cpu'] = _member(member='cpu',
                                     usage=self._usage(pilots_active=1))
        rec.members['gpu'] = _member(member='gpu')          # idle
        assert rec.state() == 'ok'

    def test_a_resource_is_idle_only_when_every_shape_is(self):
        rec = _rec(name='beta')
        rec.members['cpu'] = _member(member='cpu')
        rec.members['gpu'] = _member(member='gpu')
        assert rec.state() == 'idle'

    def test_an_unreachable_resource_reports_that(self):
        rec = _rec(name='beta', liveness=LIVENESS_LOST)
        rec.members['cpu'] = _member(member='cpu',
                                     usage=self._usage(pilots_active=1))
        assert rec.state() == 'lost'


class TestRemainingSec:
    """The countdown: recomputed on every read, never stored."""

    def test_an_allocation_answers_from_its_end_time(self):
        m = _member(end_time=time.time() + 600)
        assert m.remaining_sec() == pytest.approx(600, abs=5)

    def test_an_ended_allocation_has_nothing_left_rather_than_a_debt(self):
        m = _member(end_time=time.time() - 600)
        assert m.remaining_sec() == 0.0

    def test_it_counts_down_between_two_reads(self):
        m = _member(end_time=time.time() + 600)
        first = m.remaining_sec()
        time.sleep(0.01)
        assert m.remaining_sec() < first

    def test_a_submit_member_answers_from_its_pilots(self):
        """The dispatcher's number: the max over its live pilots."""
        m = _member(usage=ResourceUsage(remaining_sec=1234.0))
        assert m.remaining_sec() == 1234.0

    def test_a_member_with_no_pilot_and_no_allocation_says_nothing(self):
        assert _member().remaining_sec() is None

    def test_the_wire_carries_it_beside_the_unchanged_names(self):
        wire = _member(end_time=time.time() + 60, pilot='endpoint',
                       endpoint='ep_alloc').to_wire()
        # added, never renamed (Orbit plan 122)
        assert wire['member']    == 'gpu'
        assert wire['pool_name'] == 'fed-gpu'
        assert wire['pilot']     == 'endpoint'
        assert wire['endpoint']  == 'ep_alloc'
        assert wire['end_time']  == pytest.approx(time.time() + 60, abs=5)
        assert wire['remaining_sec'] == pytest.approx(60, abs=5)

    def test_the_new_fields_round_trip_through_the_wire(self):
        m = _member(pilot='endpoint', endpoint='ep_alloc',
                    end_time=1757000000.0)
        assert member_from_dict(m.to_wire()) == m


class TestPilotModeMigration:
    """A ``state.json`` written by a 121 broker has members with no ``pilot``.

    Defaulting those to ``submit`` would make the first re-POST after the
    upgrade ask for a batch job on the compute node the endpoint is already
    sitting on — exactly the second endpoint plan 122 removed.
    """

    def _raw(self, mode, **member_kw):
        member = {'member': 'default', 'member_id': 'alpha.default',
                  'class': 'gpu', 'pool_name': 'fed-gpu', 'queue': 'alloc',
                  'nodes': 2, 'cpus_per_node': 64, 'gpus_per_node': 4,
                  'walltime_sec': 1800}
        member.update(member_kw)
        return {'name': 'alpha', 'endpoint': 'ep0', 'mode': mode,
                'members': [member]}

    def test_an_allocation_member_loads_as_an_adopted_endpoint(self):
        m = record_from_dict(self._raw(MODE_ALLOCATION)).members['default']
        assert m.pilot    == 'endpoint'
        assert m.endpoint == 'ep0'

    def test_a_login_member_keeps_submitting(self):
        m = record_from_dict(self._raw(MODE_LOGIN)).members['default']
        assert m.pilot == 'submit'

    def test_an_allocation_member_is_always_the_endpoint(self):
        """The mode says what it is: even a stored `submit` is migrated --
        a join never writes one for an allocation."""
        raw = self._raw(MODE_ALLOCATION, pilot='submit')
        assert record_from_dict(raw).members['default'].pilot == 'endpoint'

    def test_a_migrated_allocation_member_reads_one_pilot(self):
        """Migrated before the member is built, so the endpoint member's
        forced 1/1 applies -- not the stale 0/N it was stored with."""
        raw = self._raw(MODE_ALLOCATION, min_pilots=0, max_pilots=4)
        m   = record_from_dict(raw).members['default']
        assert (m.min_pilots, m.max_pilots) == (1, 1)

    def test_the_migration_survives_a_save_and_reload(self, tmp_path):
        p  = tmp_path / 'state.json'
        st = FederationState(p)
        st.resources['alpha'] = record_from_dict(self._raw(MODE_ALLOCATION))
        st.save()
        back = FederationState(p).load().resources['alpha']
        assert back.members['default'].pilot == 'endpoint'


class TestSingleMemberDerivation:
    """A state.json written before class pools must still load."""

    _PRE08 = {
        'name': 'legacy', 'endpoint': 'ep0', 'mode': 'login',
        'site': 'NERSC', 'kind': 'hpc',
        'capabilities': {'cores': 128, 'gpus': 4, 'mem_gb': 256,
                         'software': ['lammps', 'pytorch']},
        'budget': {'node_hours': 40.0},
        'scratch_base': '/tmp/fed/legacy',
        'dispatcher_sid': 'fed-legacy', 'pool_name': 'fed-legacy',
        'pool_config': {
            'name': 'fed-legacy', 'endpoint_name': 'ep0',
            'queue': 'regular', 'account': 'm1234',
            'min_pilots': 0, 'max_pilots': 2, 'default_size': 'default',
            'pilot_sizes': {'default': {'nodes': 2, 'cpus_per_node': 64,
                                        'gpus_per_node': 4,
                                        'walltime_sec': 1800,
                                        'rhapsody_backend': 'concurrent'}}},
    }

    def test_exactly_one_member_is_derived(self):
        rec = record_from_dict(dict(self._PRE08))
        assert list(rec.members) == ['default']

    def test_the_derived_member_carries_the_stored_pool_shape(self):
        m = record_from_dict(dict(self._PRE08)).members['default']
        assert m.member_id  == 'legacy.default'
        assert m.queue      == 'regular'
        assert m.account    == 'm1234'
        assert (m.nodes, m.cpus_per_node, m.gpus_per_node,
                m.walltime_sec) == (2, 64, 4, 1800)
        assert m.max_pilots == 2
        assert m.rhapsody_backend == 'concurrent'
        assert m.scratch_base == '/tmp/fed/legacy'
        assert m.shared_fs is True

    def test_the_derived_member_is_classified_by_its_gpus(self):
        m = record_from_dict(dict(self._PRE08)).members['default']
        assert m.cls       == 'gpu'
        assert m.pool_name == 'fed-gpu'

        cpu = dict(self._PRE08)
        cpu['pool_config'] = dict(cpu['pool_config'])
        cpu['pool_config']['pilot_sizes'] = {
            'default': {'nodes': 1, 'cpus_per_node': 8, 'gpus_per_node': 0,
                        'walltime_sec': 3600,
                        'rhapsody_backend': 'concurrent'}}
        m2 = record_from_dict(cpu).members['default']
        assert (m2.cls, m2.pool_name) == ('cpu', 'fed-cpu')

    def test_a_pre08_login_record_still_submits_its_pilots(self):
        m = record_from_dict(dict(self._PRE08)).members['default']
        assert (m.pilot, m.endpoint) == ('submit', 'ep0')

    def test_a_pre08_allocation_record_becomes_an_adopted_endpoint(self):
        """Upgraded in place: its endpoint is inside the allocation, which
        is exactly what the second, submitted pilot stood in for."""
        raw = dict(self._PRE08)
        raw['mode'] = MODE_ALLOCATION
        m = record_from_dict(raw).members['default']
        assert (m.pilot, m.endpoint) == ('endpoint', 'ep0')
        assert (m.min_pilots, m.max_pilots) == (1, 1)

    def test_the_derived_member_inherits_budget_software_attributes(self):
        m = record_from_dict(dict(self._PRE08)).members['default']
        assert m.budget   == {'node_hours': 40.0}
        assert m.software == ['lammps', 'pytorch']
        assert m.attributes == {'site': 'NERSC', 'kind': 'hpc',
                                'mem_gb_per_node': 256}

    def test_a_thin_pre08_record_declares_no_empty_attributes(self):
        # BLOCKING regression: a record joined without site/kind and with no
        # discovered mem_gb used to derive {'site': '', 'kind': '',
        # 'mem_gb_per_node': None}.  The dispatcher's parse_member refuses a
        # non string/number/list attribute value — and because every
        # registration re-sends the FULL member list, one such record would
        # 400 *every* later join and every restart replay, not just its own.
        raw = dict(self._PRE08)
        raw['site'] = ''
        raw['kind'] = ''
        raw['capabilities'] = {'cores': 128, 'software': []}
        m = record_from_dict(raw).members['default']
        assert m.attributes == {}
        assert all(v is not None and v != ''
                   for v in m.match_attributes().values()
                   if not isinstance(v, list))

    def test_a_partly_declared_pre08_record_keeps_what_it_has(self):
        raw = dict(self._PRE08)
        raw['kind'] = ''
        raw['capabilities'] = {'cores': 128, 'mem_gb': 0}
        m = record_from_dict(raw).members['default']
        assert m.attributes == {'site': 'NERSC', 'mem_gb_per_node': 0}

    def test_a_record_without_a_pool_config_falls_back_to_the_pool_block(self):
        raw = dict(self._PRE08)
        raw.pop('pool_config')
        raw['pool'] = {'queue': 'regular', 'nodes': 3, 'cpus_per_node': 16,
                       'gpus_per_node': 0, 'walltime_sec': 600}
        m = record_from_dict(raw).members['default']
        assert (m.queue, m.nodes, m.cpus_per_node) == ('regular', 3, 16)
        assert m.cls == 'cpu'

    def test_an_explicit_members_list_wins(self):
        raw = dict(self._PRE08)
        raw['members'] = [_member().to_wire()]
        rec = record_from_dict(raw)
        assert list(rec.members) == ['gpu']
        assert rec.members['gpu'].member_id == 'beta.gpu'

    def test_members_survive_persistence(self, tmp_path):
        p   = tmp_path / 'state.json'
        st  = FederationState(p)
        rec = _rec(name='beta', mode=MODE_LOGIN)
        rec.members['gpu'] = _member()
        st.resources['beta'] = rec
        st.save()
        back = FederationState(p).load().resources['beta']
        # everything survives verbatim -- except that a member written
        # before members carried their own endpoint is given the record's
        assert back.members['gpu'] == _member(endpoint='ep0')


class TestResourceAttributes:
    """The one helper both member-synthesis paths share."""

    def test_a_full_declaration_maps_straight_through(self):
        assert resource_attributes('NERSC', 'hpc', 256) == \
            {'site': 'NERSC', 'kind': 'hpc', 'mem_gb_per_node': 256}

    def test_none_and_empty_values_are_dropped(self):
        assert resource_attributes('', '', None) == {}
        assert resource_attributes('NERSC', '', None) == {'site': 'NERSC'}

    def test_a_zero_is_a_declaration_not_an_absence(self):
        assert resource_attributes(mem_gb=0) == {'mem_gb_per_node': 0}

    def test_the_defaults_declare_nothing(self):
        assert resource_attributes() == {}


class TestAggregate:

    def _two_member(self) -> ResourceRecord:
        rec = _rec(name='beta', capabilities={'mem_gb': 64,
                                              'cores': 1, 'gpus': 0},
                   budget={'node_hours': 1.0})
        rec.members['cpu'] = _member(member='cpu', cls='cpu', gpus_per_node=0,
                                     nodes=2, cpus_per_node=128,
                                     software=['lammps'],
                                     budget={'node_hours': 20.0})
        rec.members['gpu'] = _member(member='gpu', nodes=1, cpus_per_node=64,
                                     gpus_per_node=8, software=['pytorch'],
                                     budget={'node_hours': 8.0})
        return rec

    def test_cores_and_gpus_are_summed_over_the_members(self):
        rec = self._two_member()
        rec.aggregate()
        assert rec.capabilities['cores'] == 2 * 128 + 1 * 64
        assert rec.capabilities['gpus']  == 1 * 8

    def test_software_is_the_union_in_declaration_order(self):
        rec = self._two_member()
        rec.aggregate()
        assert rec.capabilities['software'] == ['lammps', 'pytorch']

    def test_other_declared_capabilities_are_left_alone(self):
        # an operator's mem_gb is a statement about the machine; the
        # aggregate answers a different question and must not overwrite it
        rec = self._two_member()
        rec.aggregate()
        assert rec.capabilities['mem_gb'] == 64

    def test_budget_is_the_sum_of_the_member_budgets(self):
        rec = self._two_member()
        rec.aggregate()
        assert rec.budget == {'node_hours': 28.0}

    def test_a_member_less_record_is_untouched(self):
        rec = _rec()
        before = dict(rec.capabilities)
        rec.aggregate()
        assert rec.capabilities == before

    def test_to_wire_renders_members_as_a_list(self):
        rec = self._two_member()
        wire = rec.to_wire()
        assert [m['member'] for m in wire['members']] == ['cpu', 'gpu']
        assert wire['members'][1]['class'] == 'gpu'


# ---------------------------------------------------------------------------
# Ledger: placement is late, and survives a leave
# ---------------------------------------------------------------------------

class TestLedgerPlacement:

    def _state(self, tmp_path) -> FederationState:
        st = FederationState(tmp_path / 'state.json')
        st.resources['a'] = _rec(name='a')
        for tid, res, mid, state in (
                ('t.run',  'a', 'a.default', 'RUNNING'),
                ('t.q',    'a', None,        'QUEUED'),
                ('t.done', 'a', 'a.default', 'DONE'),
                ('t.other', 'b', 'b.default', 'RUNNING')):
            st.ledger[tid] = SubmitLedgerEntry(
                task_id=tid, resource=res, member_id=mid, state=state,
                pool='fed-cpu', dispatcher_sid='fed', cls='cpu')
        return st

    def test_member_counts_only_see_placed_tasks(self, tmp_path):
        st = self._state(tmp_path)
        # t.q has no member yet: it counts on the resource, not on a member
        assert st.member_task_counts('a.default') == (1, 1, 0)
        assert st.task_counts('a')                == (2, 1, 0)

    def test_leave_keeps_live_entries_and_re_points_them(self, tmp_path):
        st = self._state(tmp_path)
        st.drop_resource('a', keep_active=True)
        assert 'a' not in st.resources
        assert set(st.ledger) == {'t.run', 't.q', 't.other'}   # t.done gone
        for tid in ('t.run', 't.q'):
            entry = st.ledger[tid]
            assert entry.resource  is None
            assert entry.member_id is None
            # the class pool and the fed session outlive the resource
            assert entry.pool           == 'fed-cpu'
            assert entry.dispatcher_sid == 'fed'
        assert st.ledger['t.other'].resource == 'b'

    def test_a_full_teardown_drops_everything(self, tmp_path):
        st = self._state(tmp_path)
        st.drop_resource('a', keep_active=False)
        assert set(st.ledger) == {'t.other'}

    def test_the_ledger_round_trips_its_placement(self, tmp_path):
        st = self._state(tmp_path)
        st.save()
        back = FederationState(st.path).load().ledger['t.run']
        assert back.member_id == 'a.default'
        assert back.cls       == 'cpu'

    def test_a_null_resource_persists(self, tmp_path):
        st = self._state(tmp_path)
        st.drop_resource('a', keep_active=True)
        st.save()
        assert FederationState(st.path).load().ledger['t.q'].resource is None


# ---------------------------------------------------------------------------
# Member validators
# ---------------------------------------------------------------------------

class TestMemberValidators:

    def test_member_name_pattern(self):
        assert validate_member_name('gpu-1_a') == 'gpu-1_a'
        # a dot is the resource/member separator and must not appear here
        for bad in ('', None, 'Upper', 'has space', 'a.b', '-lead', 42):
            with pytest.raises(FederationStateError):
                validate_member_name(bad)

    def test_class_is_refused_not_coerced(self):
        assert validate_class('gpu') == 'gpu'
        with pytest.raises(FederationStateError, match='never coerced'):
            validate_class('GPU')
        for bad in ('', None, 'a b', 'a.b', '_x'):
            with pytest.raises(FederationStateError):
                validate_class(bad)

    def test_attributes_accept_strings_numbers_and_string_lists(self):
        assert validate_attributes({'site': 'NERSC', 'mem_gb_per_node': 256,
                                    'tags': ['a', 'b']}) == \
            {'site': 'NERSC', 'mem_gb_per_node': 256, 'tags': ['a', 'b']}

    def test_attributes_drop_nulls_and_reject_the_rest(self):
        assert validate_attributes({'site': None}) == {}
        assert validate_attributes(None) == {}
        for bad in ([], {'k': True}, {'k': {'nested': 1}}, {'k': [1]},
                    {'': 'v'}):
            with pytest.raises(FederationStateError):
                validate_attributes(bad)

    def test_software_is_a_list_of_strings(self):
        assert validate_software(None) == []
        assert validate_software(['a']) == ['a']
        for bad in ('lammps', [1], {}):
            with pytest.raises(FederationStateError):
                validate_software(bad)

    def test_pool_int_message_can_name_a_member(self):
        with pytest.raises(FederationStateError, match="'member gpu.nodes'"):
            validate_pool_int({}, 'nodes', minimum=1, label='member gpu')
