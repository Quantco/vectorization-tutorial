#!/bin/sh

curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
eval "$(./bin/micromamba shell hook -s bash)"
micromamba create -y -n vectorization -f conda-lock.yml
micromamba activate vectorization