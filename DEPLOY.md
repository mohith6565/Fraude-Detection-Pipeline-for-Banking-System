# Deploying Fraud Atlas to GitHub (CI/CD)

This document contains copy-paste commands to create a GitHub repository, push this project, add required secrets, and trigger the CI/CD workflow added at `.github/workflows/ci-cd.yml`.

Prerequisites
- `git` installed and configured
- `gh` (GitHub CLI) recommended for convenience (https://cli.github.com/)
- Docker Hub account (if you want to push images)
- Optional: `kubectl` and cluster access (kubeconfig) if you want automatic K8s deploy

1) Create the repository and push (recommended, using `gh`)

Linux / macOS / WSL
```bash
# from repo root
gh repo create YOUR_USER/REPO_NAME --public --source=. --remote=origin --push
git push -u origin main
```

Windows PowerShell (with `gh` authenticated)
```powershell
# from repo root
gh repo create YOUR_USER/REPO_NAME --public --source=. --remote=origin --push
git push -u origin main
```

If you prefer to create the repo on github.com manually, add the `origin` remote and push:
```bash
git remote add origin git@github.com:YOUR_USER/REPO_NAME.git
git branch -M main
git push -u origin main
```

2) Add required repository secrets (Settings → Secrets → Actions)
- `DOCKERHUB_USERNAME` — Docker Hub username
- `DOCKERHUB_TOKEN` — Docker Hub access token (create at hub.docker.com → Account Settings → Security → New Access Token)
- `KUBE_CONFIG_DATA` — (optional) base64-encoded kubeconfig for Kubernetes deployment

Create secrets with `gh` (example):

Linux / macOS / WSL
```bash
gh secret set DOCKERHUB_USERNAME --body "your-dockerhub-username"
gh secret set DOCKERHUB_TOKEN --body "$(cat ~/dockerhub_token.txt)"

# If you have kubeconfig file and want to deploy to k8s automatically
base64 -w0 ~/.kube/config | gh secret set KUBE_CONFIG_DATA
```

Windows PowerShell (base64 encode kubeconfig):
```powershell
#$kube = Get-Content $HOME\.kube\config -Raw
#$b64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($kube))
#echo $b64 | gh secret set KUBE_CONFIG_DATA
```

3) Trigger the workflow
- Push to `main`, or open a PR targeting `main`. The workflow runs on pushes/PRs to `main`.

4) Check Actions and deployment
- Go to the GitHub repository → Actions → view the workflow run logs.
- After `build_and_push` completes, verify Docker Hub / GHCR for the `fraud-atlas:latest` image.
- If `KUBE_CONFIG_DATA` was set, the `deploy_k8s` job applies manifests in `k8s/` and updates `deployment/fraud-atlas-web` image.

Verification commands (locally / with access):
```bash
# Check pods
kubectl get pods -l app=fraud-atlas-web

# Tail logs
kubectl logs -l app=fraud-atlas-web -c web -f

# Check Docker image (Docker Hub/GHCR web UI)
```

Helper scripts
- `scripts/create_and_push_repo.sh` and `scripts/create_and_push_repo.ps1` in this repo provide a quick GH-based creation + secret examples.

If you want, I can:
- create a branch that includes a sample `kubeconfig` placeholder (NOT real credentials)
- or run through these steps with you interactively while you supply secrets.
