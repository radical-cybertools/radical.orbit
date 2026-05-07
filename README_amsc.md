
This page only covers what is **AMSC-specific**.  For bridge / cert /
URL setup see the main `README.md` ("Bridge configuration").

## `$AMSC_DIR`

The AMSC demos and the install script honour `$AMSC_DIR` (defaulting
to `$HOME/.amsc`) for everything they place under the AMSC layout:
the per-target venv, the IRI bearer tokens, and the wrapper-script
path that PsiJ / IRI submissions point at.  Set it in your `.bashrc`
to relocate, or leave unset for the default.

```sh
export AMSC_DIR="$HOME/.amsc"   # default — set only if relocating
```

The remainder of this page uses `${AMSC_DIR:-$HOME/.amsc}` in shell
snippets so they paste-and-run regardless of whether the env var is
set.

Note that  for machines where you don't use the default, you need to set the
corresponding values in the info dicts in `amsc.py` and `matey.py`.

## Per-target install (script form)

```sh
mkdir -p "$AMSC_DIR"
cd       "$AMSC_DIR"

rm -rf ve/
python3.11 -m venv ./ve
. ve/bin/activate

pip install --upgrade pip
pip install dragonhpc scikit-learn mpi4py

pip install --upgrade pip pytest pytest-asyncio
pip install --upgrade dragonhpc scikit-learn mpi4py psij-python

git clone git@github.com:radical-cybertools/radical.edge      || true
git clone git@github.com:radical-cybertools/radical.asyncflow || true
git clone git@github.com:radical-cybertools/rhapsody          || true
git clone git@github.com:radical-cybertools/rose              || true

cd radical.edge/     ; git checkout feature/amsc; git pull; pip install .; cd ..
cd rhapsody/         ; git checkout feature/edge; git pull; pip install .; cd ..
cd radical.asyncflow/; git checkout feature/edge; git pull; pip install .; cd ..
cd rose/             ; git checkout tmp_am/raas ; git pull; pip install .; cd ..

which python3
python3 -V
```

## IRI bearer tokens (client side only)

Tokens live on the host that runs `amsc.py` / `matey.py`, not on the
remote targets:

```sh
mkdir -p "$AMSC_DIR"

# NERSC IRI (Globus access token; refresh per Globus client docs)
echo "$NERSC_GLOBUS_TOKEN" > "$AMSC_DIR/token_nersc"

# OLCF open IRI (S3M token)
echo "$OLCF_S3M_TOKEN"     > "$AMSC_DIR/token_olcf"

chmod 600 "$AMSC_DIR"/token_*
```

## Run the demo

```sh
# Bridge with the iri_connect plugin loaded so the demos can spawn
# child edges via NERSC / OLCF IRI:
radical-edge-bridge.py -p iri_connect,staging,sysinfo

# (Optional, separate terminal) Bring up any pre-running edges to
# include in the target picker:
ssh <target> "${AMSC_DIR:-\$HOME/.amsc}/ve/bin/radical-edge-wrapper.sh -n <target>"

# Then, from the bridge host:
python examples/amsc.py    # toy ROSE active learning across heterogeneous edges
python examples/matey.py   # MATEY-driven workflow (under development)
```

Both drivers are interactive: they list the viable targets
(connected edges + IRI endpoints with valid tokens) and let you tick
the ones to use.  They run a ROSE workflow on the **first** edge
that comes up and tear down only what they themselves submitted.

