# bin/

The only real script here is `bin/searchGlacier.py`, which is included mainly for historical and reference reasons.

The symlinks in this directory are a convenience for developers.
They should not be used in production.
They allow developers to easily run the entry points in the dev environment without activating it.
This assumes that the dev environment is `../aws-object-search-dev`.
The `env` symlink at the root of the repo is a part of this process.
