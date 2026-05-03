The `gh` CLI cannot create classic PATs programmatically — GitHub's API doesn't expose that endpoint. But `gh` handles the important part: saving the secret without the token ever touching your shell history. Here's the exact sequence:

---

**Step 1 — Open the token creation page in your browser**

```bash
xdg-open "https://github.com/settings/tokens/new?scopes=repo,workflow&description=ACTION_PAT"
```

This pre-fills the correct scopes. In the browser:
- Expiration: set to "No expiration" (or 1 year — your call)
- Confirm `repo` and `workflow` are ticked — nothing else is needed
- Click **Generate token**
- Copy the `ghp_...` value immediately (GitHub shows it only once)

---

**Step 2 — Save it as a repository secret using `gh`**

```bash
gh secret set ACTION_PAT --repo G2F-data/g2f-platform-harvester
```

`gh` will prompt you to paste the token value interactively. It reads from stdin without echoing, so the value never appears in your terminal or shell history.

---

**Step 3 — Verify it was saved**

```bash
gh secret list --repo G2F-data/g2f-platform-harvester
```

You should see `ACTION_PAT` in the list with an updated timestamp. You can't read the value back (by design), but presence in the list confirms it's set.

---

That's all that's needed. Once `ACTION_PAT` is in place, the chainer job in the workflow will use it and the 403 is gone.
