"""Unit tests for bin/radical-edge-makeflow-prep.

Exercises the preprocessor's parser, directive scoping, rewrite
semantics, and error reporting.  The script has no ``.py`` extension
so we load it via ``importlib`` / ``SourceFileLoader``.
"""

import sys
import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest


_loader = SourceFileLoader(
    'prep_mod',
    str(Path(__file__).resolve().parents[2]
        / 'bin' / 'radical-edge-makeflow-prep')
)
_spec = importlib.util.spec_from_loader('prep_mod', _loader)
_PREP = importlib.util.module_from_spec(_spec)
sys.modules['prep_mod'] = _PREP
_loader.exec_module(_PREP)

PrepOptions = _PREP.PrepOptions
PrepError   = _PREP.PrepError
prep_stream = _PREP.prep_stream


def _run(text: str, **opts_kwargs) -> str:
    opts = PrepOptions(**opts_kwargs)
    lines = text.splitlines(keepends=True)
    return ''.join(prep_stream(lines, 'runid0', opts))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestBasicRewrite:

    def test_minimal_edge(self):
        out = _run(
            'EDGE = "e1"\n'
            '\n'
            'out.dat: in.dat\n'
            '\t./compute in.dat out.dat\n')
        assert 'radical-edge-run' in out
        assert '--edge=e1' in out
        assert '--pool=' not in out
        assert '--run-id=runid0' in out
        # Edge mode does NOT propagate --in/--out (staging unsupported).
        assert '--in '  not in out
        assert '--out ' not in out
        # Original command is wrapped in sh -c to contain shell grammar
        assert "-- sh -c './compute in.dat out.dat'" in out

    def test_minimal_pool(self):
        out = _run(
            'POOL = "p1"\n'
            '\n'
            'out.dat: in.dat\n'
            '\t./compute in.dat out.dat\n')
        assert 'radical-edge-run' in out
        assert '--pool=p1' in out
        assert '--edge=' not in out
        assert '--in in.dat' in out
        assert '--out out.dat' in out
        assert "-- sh -c './compute in.dat out.dat'" in out

    def test_directives_consumed(self):
        out = _run(
            'POOL = "p1"\n'
            'out.dat: in.dat\n\t./foo\n')
        # POOL line is stripped from output
        assert 'POOL' not in out

    def test_priority_passed_through(self):
        out = _run(
            'POOL = "p"\nPRIORITY = 42\n'
            'o: i\n\tcmd\n')
        assert '--priority=42' in out

    def test_no_priority_defaults_to_zero(self):
        out = _run('POOL = "p"\no: i\n\tcmd\n')
        assert '--priority=0' in out


# ---------------------------------------------------------------------------
# Scoping
# ---------------------------------------------------------------------------

class TestScoping:

    def test_scope_applies_to_subsequent_rules(self):
        out = _run(
            'POOL = "p1"\n'
            'a: i1\n\tc1\n'
            'POOL = "p2"\n'
            'b: i2\n\tc2\n')
        lines = [l for l in out.split('\n') if 'radical-edge-run' in l]
        assert len(lines) == 2
        assert '--pool=p1' in lines[0]
        assert '--pool=p2' in lines[1]

    def test_default_edge_option(self):
        out = _run('o: i\n\tcmd\n', default_edge='eD')
        assert '--edge=eD' in out

    def test_default_pool_option(self):
        out = _run('o: i\n\tcmd\n', default_pool='pD')
        assert '--pool=pD' in out

    def test_explicit_overrides_default_edge(self):
        out = _run('EDGE = "eX"\n'
                   'o: i\n\tcmd\n',
                   default_edge='eD')
        assert '--edge=eX' in out
        assert '--edge=eD' not in out

    def test_explicit_pool_overrides_default_edge(self):
        '''Setting POOL in the file clears the --default-edge scope.'''
        out = _run('POOL = "pX"\n'
                   'o: i\n\tcmd\n',
                   default_edge='eD')
        assert '--pool=pX' in out
        assert '--edge=' not in out


# ---------------------------------------------------------------------------
# EDGE / POOL mutual exclusion
# ---------------------------------------------------------------------------

class TestEdgePoolMutex:

    def test_pool_clears_edge_scope(self):
        '''Setting POOL after EDGE in the same scope clears EDGE.'''
        out = _run(
            'EDGE = "e1"\n'
            'POOL = "p1"\n'
            'o: i\n\tcmd\n')
        assert '--pool=p1' in out
        assert '--edge=' not in out

    def test_edge_clears_pool_scope(self):
        '''Setting EDGE after POOL in the same scope clears POOL.'''
        out = _run(
            'POOL = "p1"\n'
            'EDGE = "e1"\n'
            'o: i\n\tcmd\n')
        assert '--edge=e1' in out
        assert '--pool=' not in out

    def test_per_rule_switching(self):
        '''Two rules with different EDGE/POOL — each gets its own.'''
        out = _run(
            'EDGE = "e1"\n'
            'a: i1\n\tc1\n'
            'POOL = "p1"\n'
            'b: i2\n\tc2\n')
        lines = [l for l in out.split('\n') if 'radical-edge-run' in l]
        assert len(lines) == 2
        assert '--edge=e1' in lines[0]
        assert '--pool=' not in lines[0]
        assert '--pool=p1' in lines[1]
        assert '--edge=' not in lines[1]

    def test_cli_default_mutex(self):
        '''--default-edge and --default-pool cannot both be set.'''
        with pytest.raises(PrepError, match='mutually exclusive'):
            _run('o: i\n\tcmd\n',
                 default_edge='eD', default_pool='pD')


# ---------------------------------------------------------------------------
# --pools / POOLS directive (workflow-level)
# ---------------------------------------------------------------------------

class TestPoolsFile:

    def test_no_pools_flag_no_arg_emitted(self):
        out = _run('POOL = "p"\no: i\n\tcmd\n')
        assert '--pools=' not in out

    def test_pools_flag_emitted_on_every_rule(self):
        out = _run(
            'POOL = "p"\n'
            'a: i\n\tc1\n'
            'b: a\n\tc2\n',
            pools_file='/wf/pools.json')
        lines = [l for l in out.splitlines() if 'radical-edge-run' in l]
        assert len(lines) == 2
        for line in lines:
            assert '--pools=/wf/pools.json' in line

    def test_pools_directive_in_file(self):
        out = _run(
            'POOL = "p"\n'
            'POOLS = "/from/header.json"\n'
            'o: i\n\tcmd\n')
        assert '--pools=/from/header.json' in out

    def test_directive_overrides_default_pools(self):
        '''POOLS directive in the file wins over --pools default.'''
        out = _run(
            'POOL = "p"\n'
            'POOLS = "/from/header.json"\n'
            'o: i\n\tcmd\n',
            pools_file='/from/cli.json')
        assert '--pools=/from/header.json' in out
        assert '--pools=/from/cli.json' not in out


# ---------------------------------------------------------------------------
# Pass-through
# ---------------------------------------------------------------------------

class TestPassThrough:

    def test_comments_preserved(self):
        src = ('# a comment\nPOOL = "p"\n'
               '# another\n'
               'o: i\n\tcmd\n# after rule\n')
        out = _run(src)
        assert '# a comment' in out
        assert '# another' in out
        assert '# after rule' in out

    def test_blank_lines_preserved(self):
        src = 'POOL = "p"\n\n\no: i\n\tcmd\n\n'
        out = _run(src)
        assert out.count('\n\n') >= 2

    def test_unknown_variable_passed_through(self):
        src = ('CATEGORY = "big"\nMEMORY = 16384\n'
               'POOL = "p"\no: i\n\tcmd\n')
        out = _run(src)
        assert 'CATEGORY = "big"' in out
        assert 'MEMORY = 16384' in out


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestErrors:

    def test_rule_without_any_target(self):
        '''Without EDGE/POOL in the file or CLI default → reject.'''
        with pytest.raises(PrepError, match='EDGE or POOL'):
            _run('o: i\n\tcmd\n')

    def test_rule_with_no_command(self):
        with pytest.raises(PrepError, match='no command'):
            _run('POOL = "p"\no: i\n')

    def test_rule_header_backslash_continuation(self):
        with pytest.raises(PrepError, match='multi-line'):
            _run('POOL = "p"\no: i \\\n  j\n\tcmd\n')

    def test_priority_non_integer(self):
        with pytest.raises(PrepError, match='PRIORITY'):
            _run('PRIORITY = not-a-number\nPOOL = "p"\n'
                 'o: i\n\tcmd\n')


# ---------------------------------------------------------------------------
# Multi-command rules joined with ;
# ---------------------------------------------------------------------------

class TestMultiCommand:

    def test_two_commands_joined(self):
        out = _run(
            'POOL = "p"\n'
            'o: i\n'
            '\tpart1\n'
            '\tpart2\n')
        # Joined with ' ; ' inside the sh -c argument.
        assert "-- sh -c 'part1 ; part2'" in out

    def test_shell_metachars_contained(self):
        """Pipes and redirections must end up inside the sh -c arg so
        they are not consumed by the outer shell Makeflow uses."""
        out = _run('POOL = "p"\n'
                   'o: i\n\tcat i | grep x > o\n')
        assert "-- sh -c 'cat i | grep x > o'" in out
        # The raw pipe must not appear outside the quoted argument
        assert '| grep' not in out.replace("'cat i | grep x > o'", '')

    def test_single_quotes_in_cmd_escaped(self):
        """shlex.quote produces safe output for commands containing
        single quotes themselves."""
        out = _run('POOL = "p"\n'
                   'o: i\n\techo \'hi there\'\n')
        assert '-- sh -c' in out
        # Some quoted form ends the line; the important part is that
        # the preprocessor doesn't produce a syntactically broken line.
        # Compile-check via shlex.split:
        import shlex as _shlex
        line = [l for l in out.split('\n') if 'radical-edge-run' in l][0]
        tokens = _shlex.split(line)
        sep    = tokens.index('--')
        # After '-- sh -c', the final token should equal the original.
        assert tokens[sep + 1:sep + 3] == ['sh', '-c']
        assert tokens[sep + 3] == "echo 'hi there'"


# ---------------------------------------------------------------------------
# LOCAL keyword (Makeflow's "run on submitter" prefix)
# ---------------------------------------------------------------------------

class TestLocalKeyword:

    def test_local_rule_not_wrapped(self):
        '''A rule whose command starts with ``LOCAL `` is left to
        Makeflow (LOCAL preserved, no radical-edge-run wrapper).
        '''
        out = _run(
            'EDGE = "e"\n'
            'o: i\n'
            '\tLOCAL ./gen.sh > o\n')
        # The LOCAL keyword is preserved and the command is NOT
        # wrapped in radical-edge-run.
        assert 'LOCAL ./gen.sh > o' in out
        assert 'radical-edge-run' not in out

    def test_non_local_rule_still_wrapped(self):
        out = _run('EDGE = "e"\n'
                   'o: i\n\t./gen.sh\n')
        assert 'radical-edge-run' in out
        assert 'LOCAL' not in out


# ---------------------------------------------------------------------------
# Makeflow $(VAR) expansion inside sh -c args
# ---------------------------------------------------------------------------

class TestMakeflowVarRewrite:

    def test_var_expanded_in_command(self):
        '''Makeflow's $(VAR) inside the rule command is expanded
        in-place using the values captured from VAR=value
        assignments earlier in the file.  Needed because we wrap the
        command in sh -c '...' (blocks makeflow's substitution) AND
        we can't rely on env inheritance at the remote executor.
        '''
        out = _run(
            'SRC = "/in"\n'
            'DST = "/out"\n'
            'EDGE = "e"\n'
            'o: i\n'
            '\tcp $(SRC)/file $(DST)/file\n')
        line = [l for l in out.splitlines() if 'radical-edge-run' in l][0]
        assert '$(SRC)' not in line
        assert '$(DST)' not in line
        assert '/in/file'  in line
        assert '/out/file' in line

    def test_unknown_var_left_alone(self):
        '''Unknown vars stay as $(VAR) so the user notices instead
        of getting silent empty expansions.
        '''
        out = _run(
            'EDGE = "e"\n'
            'o: i\n'
            '\tcp $(NOSUCH)/x.txt /tmp/\n')
        line = [l for l in out.splitlines() if 'radical-edge-run' in l][0]
        assert '$(NOSUCH)' in line

    def test_local_rule_not_expanded(self):
        '''LOCAL rules pass through to makeflow, which does its own
        $(VAR) substitution — don't touch them.
        '''
        out = _run(
            'LOG = "/logs"\n'
            'EDGE = "e"\n'
            'o: i\n'
            '\tLOCAL ./gen.sh > $(LOG)/out.log\n')
        assert '$(LOG)' in out
        # And the LOG assignment itself is passed through
        assert 'LOG = "/logs"' in out


# ---------------------------------------------------------------------------
# run_id derivation
# ---------------------------------------------------------------------------

class TestRunId:

    def test_run_id_deterministic(self, tmp_path: Path):
        p = tmp_path / 'wf.makeflow'
        p.write_text('POOL = "p"\n')
        r1 = _PREP.compute_run_id(p)
        r2 = _PREP.compute_run_id(p)
        assert r1 == r2

    def test_run_id_changes_on_mtime(self, tmp_path: Path):
        import os, time
        p = tmp_path / 'wf.makeflow'
        p.write_text('POOL = "p"\n')
        r1 = _PREP.compute_run_id(p)
        time.sleep(0.01)
        os.utime(p, None)   # bumps mtime
        r2 = _PREP.compute_run_id(p)
        assert r1 != r2
