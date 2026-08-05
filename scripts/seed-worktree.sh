#!/usr/bin/env bash
#
# Seed a git worktree with the large, gitignored data the targets pipeline needs.
#
# A worktree is a checkout of tracked files only, so a new one has all the code
# and none of the data. Copying it from the main checkout gives every worktree a
# fully writable copy of the pipeline state, and that isolation is the point: a
# branch that changes pipeline code rewrites its own store rather than
# invalidating the main checkout's or colliding with a build running there.
#
# This repo lives on ext4, which has no reflinks, so a copy here is real bytes.
# The two seed lists are therefore moved differently:
#
#   * Immutable inputs (data/, renv/library) are hardlinked. Nothing in the
#     pipeline writes under either path — data/ holds hand-downloaded CNEFE and
#     TSE extracts, and renv replaces a package directory rather than editing
#     files inside it — so one inode can serve every checkout, and 6GB of inputs
#     cost nothing per worktree. They are also topped up file by file every
#     session and never overwritten: without that, a reused worktree quietly
#     lacks downloads the main checkout has and the pipeline refetches them.
#   * The stores and the output they index are copied byte for byte, because the
#     pipeline does write there. They also have to stay one coherent snapshot —
#     merging newer objects into an existing store would leave meta/ and
#     objects/ disagreeing — so they are cloned whole or left alone, and
#     re-cloned only while the worktree has built nothing of its own.
#
# The failure worth designing against is a half-copied seed, because it is the
# one that hides: a store missing part of itself still looks seeded, so every
# later session leaves it alone and the worktree stays broken for as long as it
# lives. The lock and the staging directory below are the two defences.

set -euo pipefail

# The two targets stores, whose paths are declared in _targets.yaml: _targets is
# the production profile, _targets_dev the AC/RR dev profile. Rename one there
# and it has to be renamed here too — nothing ties the two files together.
#
# _targets is only its metadata, since the objects live in S3, so it costs ~2MB.
# It is seeded so tar_outdated() tells the truth, not so worktrees can run
# production — see CLAUDE.md.
STORE_DIRS="_targets _targets_dev"

# Gitignored directories holding immutable inputs, as repo-relative paths
# separated by spaces. Topped up per file, every session.
#
# renv/library is mostly symlinks into the shared renv cache under ~/.cache, so
# seeding it is nearly free — but a worktree without it has no R packages at
# all, and renv::restore() does not work in this project.
TOPUP_DIRS="data renv/library"

# Gitignored directories the pipeline writes, cloned together as one snapshot.
# The stores belong here, alongside every directory their file targets point at
# — miss one and each target writing there is rebuilt in the worktree whatever
# the branch touched, because its file is simply absent. reports/ is here for
# the .html the two tar_render targets emit.
#
# Order matters: the stores go last. They are what every later run compares
# against, so a run killed partway leaves them still matching the stamp, which is
# what makes the next run re-clone rather than call the mixture seeded.
SNAPSHOT_DIRS="output reports $STORE_DIRS"

repo_root=$(git rev-parse --show-toplevel)

# The first entry of `git worktree list` is always the main checkout, which owns
# the data everything else is seeded from.
main_root=$(git worktree list --porcelain | sed -n '1s/^worktree //p')

if [ "$repo_root" = "$main_root" ]; then
  exit 0
fi

# A store's metadata file changes when a target is built and at no other time, so
# comparing copies of it says which build that store is carrying. The stamp holds
# the copies this worktree was handed when it was last seeded: while its own
# stores still match them, nothing has been built here and the snapshot is
# disposable. Both stores are stamped, so that a production build made in a
# worktree is not thrown away on the strength of the dev store alone.
stamp_dir="$repo_root/.seed-worktree-stamp"

# Opening a workspace can start this script more than once at the same moment —
# a workspace tool's setup command and the agent's session hook both run it.
# Since the copy paths replace whole directories, two overlapping runs delete
# what each other is copying. Serialise on a lock directory, which mkdir creates
# atomically or not at all. A second run has nothing to gain by waiting, since
# the run holding the lock does its work for it.
lock_dir="$repo_root/.seed-worktree-lock"
# Only ever one staging path at a time: each snapshot directory is staged,
# moved into place, and the next one started.
staged=""
probe=""
probe_out=""

cleanup() {
  rm -rf "$lock_dir"
  [ -n "$probe" ] && rm -f "$probe"
  [ -n "$probe_out" ] && rm -f "$probe_out"
  [ -n "$staged" ] && rm -rf "$staged"
  return 0
}

if ! mkdir "$lock_dir" 2>/dev/null; then
  holder=$(cat "$lock_dir/pid" 2>/dev/null || true)
  if [ -z "$holder" ]; then
    # The holder creates the lock and then writes its pid into it, so an empty
    # lock usually means it is mid-way between the two. Give it a moment before
    # concluding anything: treating that instant as an abandoned lock is exactly
    # how two runs would end up copying at once.
    sleep 1
    holder=$(cat "$lock_dir/pid" 2>/dev/null || true)
  fi
  if [ -n "$holder" ] && kill -0 "$holder" 2>/dev/null; then
    # Pids are recycled, so this can name something else entirely. Say where the
    # lock is, so a wedged worktree is one deletion from working.
    echo "seed-worktree: another seed (pid $holder) is running here; leaving it to finish."
    echo "seed-worktree: if none is, remove $lock_dir and re-run."
    exit 0
  fi
  # Nobody holds it: a previous run was killed — a session hook that outran its
  # timeout, most likely. It staged its copies rather than writing them in
  # place, so nothing here is half-replaced and the lock is safe to take over.
  # Claim it by renaming, which only one taker-over can win; losing either step
  # means someone else got there first.
  stale_lock="$lock_dir.stale.$$"
  if mv "$lock_dir" "$stale_lock" 2>/dev/null; then
    rm -rf "$stale_lock"
  fi
  if ! mkdir "$lock_dir" 2>/dev/null; then
    echo "seed-worktree: another seed is claiming the lock here; leaving it to finish."
    exit 0
  fi
  echo "seed-worktree: took over a lock left by pid ${holder:-unknown}, which is no longer running." >&2
fi
trap cleanup EXIT
echo $$ > "$lock_dir/pid"

# A directory counts as seeded once it holds gitignored content — the pipeline
# bulk. Tracked files inside it (a .gitkeep placeholder, a README, a codebook)
# arrive with the checkout and say nothing about whether the data is here, and
# --no-empty-directory keeps an empty one from passing for the data either.
holds_ignored_content() {
  local root=$1 rel=$2
  [ -n "$(git -C "$root" ls-files --others --ignored --exclude-standard --directory --no-empty-directory -- "$rel")" ]
}

stores_present() {
  local root=$1 rel
  for rel in $STORE_DIRS; do
    [ -f "$root/$rel/meta/meta" ] || return 1
  done
}

# Whether a checkout's snapshot is whole, which is a stronger question than
# whether it holds something. A store whose objects arrived but whose meta/ did
# not is a directory full of gigabytes that targets reads as an empty store,
# and "it holds content" says it is seeded, forever.
snapshot_is_whole() {
  local root=$1 rel
  for rel in $SNAPSHOT_DIRS; do
    holds_ignored_content "$root" "$rel" || return 1
  done
  stores_present "$root"
}

for rel in $TOPUP_DIRS $SNAPSHOT_DIRS; do
  case "$rel" in
    /* | *..*)
      echo "seed-worktree: seed directories take repo-relative paths; got '$rel'." >&2
      exit 1
      ;;
  esac
  if [ ! -d "$main_root/$rel" ]; then
    echo "seed-worktree: the main checkout has no directory at $rel; fix the seed lists at the top of this script." >&2
    exit 1
  fi
done

# Whether this worktree holds builds of its own, in any store. The stamp is what
# a store was handed; the main checkout's is what it would be handed now. A store
# matching either holds nothing that re-cloning would not reproduce — anything
# else was built here, and is not this script's to replace.
worktree_has_built() {
  local rel
  for rel in $STORE_DIRS; do
    [ -f "$repo_root/$rel/meta/meta" ] || continue
    if ! cmp -s "$repo_root/$rel/meta/meta" "$stamp_dir/$rel.meta" &&
      ! cmp -s "$repo_root/$rel/meta/meta" "$main_root/$rel/meta/meta"; then
      return 0
    fi
  done
  return 1
}

stores_in_step_with_main() {
  local rel
  for rel in $STORE_DIRS; do
    cmp -s "$repo_root/$rel/meta/meta" "$main_root/$rel/meta/meta" || return 1
  done
}

worktree_whole=true
snapshot_is_whole "$repo_root" || worktree_whole=false

snapshot_action="keep"
if worktree_has_built; then
  snapshot_note="left alone; it holds builds of its own, so delete these here and re-run to take the main checkout's"
  # An incomplete snapshot that also holds local builds is the one case with no
  # safe automatic answer: re-cloning would throw the builds away. Name what is
  # missing and leave the choice to whoever is here. This is what a directory
  # newly added to SNAPSHOT_DIRS looks like in a worktree that predates it.
  if [ "$worktree_whole" = false ]; then
    echo "seed-worktree: part of the snapshot here is missing — each of $SNAPSHOT_DIRS should hold data, and each of $STORE_DIRS needs its meta/meta." >&2
    echo "seed-worktree: leaving it alone, because this worktree has built a store of its own. Until the gap is filled by hand or the whole snapshot replaced, targets rebuilds everything that writes to the missing directory." >&2
  fi
elif [ "$worktree_whole" = true ] && stores_in_step_with_main; then
  snapshot_note="in step with the main checkout"
else
  snapshot_action="clone"
  # A store directory that exists at all means this is a replacement, not a first
  # seed — including a torn one, whose meta/ never arrived.
  snapshot_note="cloned from the main checkout"
  for rel in $STORE_DIRS; do
    if [ -d "$repo_root/$rel" ]; then
      snapshot_note="re-cloned: it was incomplete, or nothing had been built here and the main checkout had moved on"
      break
    fi
  done
fi

if [ "$snapshot_action" = "clone" ]; then
  # Confirm the source before touching anything, so a main checkout that has
  # never built stops the run instead of leaving the worktree with an empty store.
  if ! snapshot_is_whole "$main_root"; then
    echo "seed-worktree: the main checkout has no whole snapshot to seed from: it needs gitignored content in $SNAPSHOT_DIRS, and a meta/meta in each of $STORE_DIRS." >&2
    echo "seed-worktree: build the pipeline there first." >&2
    exit 1
  fi

  # targets records the pid of the process that last wrote a store, and the
  # record outlives the build. Both stores are checked, since either can be mid
  # build while the other is idle. Confirm the pid still belongs to an R process
  # before believing it, since a recycled one would otherwise block every session
  # from here on.
  for rel in $STORE_DIRS; do
    process_file="$main_root/$rel/meta/process"
    [ -f "$process_file" ] || continue
    builder_pid=$(sed -n 's/^pid|//p' "$process_file")
    if [ -n "$builder_pid" ] && ps -o comm= -p "$builder_pid" 2>/dev/null | grep -qiE '(^|/)r(script)?$'; then
      echo "seed-worktree: a targets pipeline (pid $builder_pid) is writing the main checkout's $rel." >&2
      echo "seed-worktree: re-run once it finishes, so the worktree gets a whole snapshot." >&2
      exit 1
    fi
  done
fi

# Every file the main checkout has and this worktree lacks. Existing files are
# never touched: the inputs are immutable, so a file already here is the file
# that belongs here.
#
# Symlinks count as files to seed. Most of renv/library is symlinks into the
# shared renv cache under an absolute path outside the repo, so they resolve
# identically from a worktree — but -type f alone would skip all of them and
# leave the library a shell of empty directories.
topup_src=()
topup_dest=()
topup_total=0
for rel in $TOPUP_DIRS; do
  while IFS= read -r -d '' src; do
    topup_total=$((topup_total + 1))
    dest="$repo_root/$rel/${src#"$main_root/$rel/"}"
    # -L as well as -e, so an existing symlink counts as present even when what
    # it points at has gone.
    if [ ! -e "$dest" ] && [ ! -L "$dest" ]; then
      topup_src+=("$src")
      topup_dest+=("$dest")
    fi
  done < <(find "$main_root/$rel" \( -type f -o -type l \) -print0)
done
topup_count=${#topup_src[@]}

# A cloud-sync client sees a seeded worktree as gigabytes of new files to upload,
# all of it reproducible from git plus the main checkout. Mark the worktree
# before copying into it, so the client never starts indexing the seed. Dropbox
# honours this attribute; on iCloud Drive or OneDrive, keep worktrees outside the
# synced folder instead.
if command -v xattr >/dev/null 2>&1; then
  case "$repo_root" in
    *Dropbox*)
      xattr -w com.dropbox.ignored 1 "$repo_root"
      if [ -d "$main_root/.claude/worktrees" ]; then
        xattr -w com.dropbox.ignored 1 "$main_root/.claude/worktrees"
      fi
      ;;
  esac
fi

# Gated so that a session with nothing to do never writes the probe scratch file
# into the main checkout — which is every session after the first.
if [ "$snapshot_action" = "clone" ] || [ "$topup_count" -gt 0 ]; then
  # Probe both copy modes with a scratch file along the real copy path, main
  # checkout to worktree, so a cross-volume worktree fails the probe rather than
  # the multi-gigabyte copy.
  probe=$(mktemp "$main_root/.reflink-probe.XXXXXX")
  probe_out="$repo_root/.reflink-probe.out.$$"

  # Reflink flags differ by platform: -c is APFS/macOS, --reflink=always is GNU
  # cp on Btrfs or XFS. ext4 has neither, which is why the inputs are hardlinked
  # rather than cloned; keep the probe so a move to Btrfs is picked up for free.
  clone_flag=""
  if cp -c "$probe" "$probe_out" 2>/dev/null; then
    clone_flag="-c"
  elif cp --reflink=always "$probe" "$probe_out" 2>/dev/null; then
    clone_flag="--reflink=always"
  fi
  rm -f "$probe_out"

  # Hardlinks need one filesystem, same as reflinks, so a worktree on another
  # volume falls back to whatever the snapshot copy uses.
  link_flag="-l"
  if ! cp -l "$probe" "$probe_out" 2>/dev/null; then
    link_flag="$clone_flag"
    echo "seed-worktree: hardlinks do not reach this worktree, so the inputs are copied instead; expect it to be slow and to consume disk." >&2
  fi
  rm -f "$probe_out"

  # -p keeps mtimes, which targets uses to skip re-hashing the store. macOS
  # clonefile preserves them regardless; the other two paths do not.
  #
  # Each directory is copied to a staging path beside its destination and moved
  # into place only once the copy has returned, so an interrupted run leaves
  # nothing a later run could read as a seeded snapshot. Staging beside the
  # destination keeps the move a rename within one filesystem.
  if [ "$snapshot_action" = "clone" ]; then
    for rel in $SNAPSHOT_DIRS; do
      staged="$repo_root/$rel.seed-incoming"
      rm -rf "$staged"
      cp $clone_flag -Rp "$main_root/$rel" "$staged"

      # Tracked files inside a snapshot directory belong to the branch, not to
      # the snapshot: reports/ holds committed .qmd sources beside the .html
      # rendered from them, and each store a committed .gitignore. Replacing the
      # directory wholesale would hand the worktree the main checkout's copies,
      # silently reverting branch edits and leaving git reporting the difference.
      # So put this worktree's own versions back over the staged ones — committed
      # or not, since an uncommitted edit is the one least recoverable.
      #
      # A tracked file the worktree no longer has on disk keeps the staged copy
      # instead of staying deleted: the common way to lose one is deleting a
      # store directory by hand, and withholding it would leave the worktree
      # dirty for good.
      while IFS= read -r -d '' tracked; do
        [ -e "$repo_root/$tracked" ] || continue
        staged_file="$staged/${tracked#"$rel/"}"
        mkdir -p "$(dirname "$staged_file")"
        cp -p "$repo_root/$tracked" "$staged_file"
      done < <(git -C "$repo_root" ls-files -z -- "$rel")

      rm -rf "${repo_root:?}/$rel"
      mv "$staged" "$repo_root/$rel"
      staged=""
    done
    mkdir -p "$stamp_dir"
    for rel in $STORE_DIRS; do
      cp $clone_flag -p "$main_root/$rel/meta/meta" "$stamp_dir/$rel.meta"
    done
  fi

  for ((i = 0; i < topup_count; i++)); do
    mkdir -p "$(dirname "${topup_dest[$i]}")"
    if [ -L "${topup_src[$i]}" ]; then
      cp -P "${topup_src[$i]}" "${topup_dest[$i]}"
    else
      cp $link_flag -p "${topup_src[$i]}" "${topup_dest[$i]}"
    fi
  done
fi

if [ "$topup_count" -gt 0 ]; then
  echo "seed-worktree: $TOPUP_DIRS: added $topup_count of $topup_total files from the main checkout."
else
  echo "seed-worktree: $TOPUP_DIRS: complete ($topup_total files)."
fi
echo "seed-worktree: $SNAPSHOT_DIRS: $snapshot_note."
