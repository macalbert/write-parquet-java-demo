# Switch default branch to 'main'

The repository currently uses `master` as the default branch. To follow current
inclusive naming conventions, please update the default branch to `main`.

Steps:
1. Create a new branch named `main` from `master` (or from the most recent
default branch).
2. Update repository settings so `main` is the default branch.
3. Update CI/CD or documentation references to `master`.
4. Remove or archive the old `master` branch if no longer needed.

This change will align the project with modern Git usage and avoid the deprecated `master` terminology.
