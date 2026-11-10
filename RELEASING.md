# Releasing

This is a checklist for releasing a new version of **stac-fastapi**.

1. Merge pull requests with Conventional Commit titles (`fix:`, `feat:`, `feat!:` for breaking changes). [release-please](https://github.com/googleapis/release-please) then opens or updates a `chore(main): release X.Y.Z` PR that bumps every version file.
   To force a version, add a `Release-As: X.Y.Z` footer to a commit merged to `main`.
2. Just before merging that PR, push a commit to it that updates [CHANGES.md](./CHANGES.md): add the header and update the links at the bottom of the file. (release-please rewrites the branch whenever new commits land on `main`.)
3. Audit CHANGES.md for completeness and accuracy.
   Also, ensure that the changes in this version are appropriate for the version number change (i.e. if you're making breaking changes, you should be increasing the `MAJOR` version number).
4. (optional) If you have permissions, run `scripts/publish --test` to test your PyPI publish.
   If successful, the published packages will be available on <http://test.pypy.org>.
5. Get approval and merge the release PR. release-please tags `X.Y.Z` and creates the GitHub release, which kicks off the [publishing workflow](.github/workflows/publish.yml).
6. Publicize the release via the appropriate social channels, including [Gitter](https://matrix.to/#/#SpatioTemporal-Asset-Catalog_python:gitter.im).

Releases from maintenance branches stay manual: bump with `uv run --with bump-my-version bump-my-version bump --new-version X.Y.Z` (it also updates `.release-please-manifest.json`), then tag and create the release by hand.
