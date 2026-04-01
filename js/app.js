/**
 * CHATUWA — app.js
 * Production-ready chat application logic.
 * Firebase Firestore + Auth + Cloudinary + WebRTC + Anthropic AI
 */

'use strict';

/* ══════════════════════════════════════════
   FIREBASE INIT
══════════════════════════════════════════ */
firebase.initializeApp({
  apiKey:            'AIzaSyDles3VcGjQBMnNdXoImBkBlUxAKUM46BE',
  authDomain:        'chat-a808f.firebaseapp.com',
  projectId:         'chat-a808f',
  storageBucket:     'chat-a808f.firebasestorage.app',
  messagingSenderId: '990586252501',
  appId:             '1:990586252501:web:320f2362d9428abe0e865c',
});

// Persist auth across page refreshes
firebase.auth().setPersistence(firebase.auth.Auth.Persistence.LOCAL).catch(() => {});

const auth = firebase.auth();
const db   = firebase.firestore();
const FS   = firebase.firestore.FieldValue;

// Cloudinary config
const CLD_CLOUD  = 'dpcuifqr1';
const CLD_PRESET = 'chatuwa';
const CLD_URL    = `https://api.cloudinary.com/v1_1/${CLD_CLOUD}/auto/upload`;

/* ══════════════════════════════════════════
   STATE
══════════════════════════════════════════ */
let ME = null;        // current user object
let isGuest = false;

let activeCh = null;  // { type, id, name, ... }

// Firestore unsubscribers
let msgUnsub, roomUnsub, dmUnsub, notifUnsub, callUnsub, typingUnsub;

// Media state
let pendingFiles = [];
let voiceBlob    = null;
let mediaRec     = null;
let recChunks    = [];
let isRec        = false;

// Call state
let pc          = null;
let localStream = null;
let callType    = null;
let callDocId   = null;
let isCaller    = false;
let isMuted     = false;
let isCamOff    = false;

// UI helpers
let urlCb           = null;
let isPrivateRoom   = false;
let roomSearchTimer = null;
let userSearchTimer = null;
let typingTimer     = null;
let editingMsgId    = null;
let editingMsgPath  = null;
let confirmResolve  = null;
let joinPendingDoc  = null;

// Pending friend requests (DB-backed)
let pendingRequests = new Set(JSON.parse(localStorage.getItem('pendingReqs') || '[]'));

// PIN lock state
let pinMode          = null;  // 'set' | 'verify'
let pinChatId        = null;
let pinChatName      = null;
let pinChatType      = null;
let pinEntry         = '';
let pinVerifyCallback = null;

/* ══════════════════════════════════════════
   HELPERS
══════════════════════════════════════════ */
const $ = id => document.getElementById(id);

/** Escape HTML entities */
const esc = s => String(s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;')
  .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

const tsNow   = () => FS.serverTimestamp();
const fmtTime = ts => ts?.toDate ? ts.toDate().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '';
const fmtSz   = b  => b < 1048576 ? (b / 1024).toFixed(1) + 'KB' : (b / 1048576).toFixed(1) + 'MB';
const dicebear = uid => `https://api.dicebear.com/7.x/thumbs/svg?seed=${encodeURIComponent(uid)}`;
const genCode  = () => Math.random().toString(36).slice(2, 8).toUpperCase();

/** Simple non-cryptographic hash for PIN */
function hashPin(pin) {
  let h = 0;
  for (let i = 0; i < pin.length; i++) { h = ((h << 5) - h) + pin.charCodeAt(i); h |= 0; }
  return String(h);
}

/* ══════════════════════════════════════════
   TOAST
══════════════════════════════════════════ */
function toast(msg, type = 'i', dur = 3200) {
  const el = document.createElement('div');
  el.className = `app-toast ${type}`;
  const icons = { s: '✓', e: '✕', i: 'ℹ' };
  el.innerHTML = `<span>${icons[type] || 'ℹ'}</span><span>${esc(msg)}</span>`;
  $('toast-stack').appendChild(el);
  setTimeout(() => {
    el.style.opacity = '0';
    el.style.transform = 'translateX(44px)';
    el.style.transition = '.3s';
    setTimeout(() => el.remove(), 320);
  }, dur);
}

/* ══════════════════════════════════════════
   CUSTOM CONFIRM DIALOG
══════════════════════════════════════════ */
function confirm2(title, msg, icon = '⚠️', okLabel = 'Confirm', okClass = '') {
  return new Promise(resolve => {
    confirmResolve = resolve;
    $('cd-icon').textContent  = icon;
    $('cd-title').textContent = title;
    $('cd-msg').textContent   = msg;
    $('cd-ok').textContent    = okLabel;
    $('cd-ok').className      = 'confirm-ok' + (okClass ? ' ' + okClass : '');
    $('confirm-dialog').classList.add('open');
  });
}
$('cd-cancel').addEventListener('click', () => {
  $('confirm-dialog').classList.remove('open');
  if (confirmResolve) { confirmResolve(false); confirmResolve = null; }
});
$('cd-ok').addEventListener('click', () => {
  $('confirm-dialog').classList.remove('open');
  if (confirmResolve) { confirmResolve(true); confirmResolve = null; }
});

/* ══════════════════════════════════════════
   OVERLAY HELPERS
══════════════════════════════════════════ */
function showOv(id) { $(id).classList.add('open'); }
function hideOv(id) { $(id).classList.remove('open'); }

/** Close overlays via [data-close] buttons */
document.querySelectorAll('[data-close]').forEach(btn =>
  btn.addEventListener('click', () => hideOv(btn.dataset.close))
);
/** Click backdrop to close */
document.querySelectorAll('.overlay').forEach(ov =>
  ov.addEventListener('click', e => { if (e.target === ov) hideOv(ov.id); })
);

/* ══════════════════════════════════════════
   THEME TOGGLE (Dark / Light)
══════════════════════════════════════════ */
function applyTheme(mode) {
  document.documentElement.dataset.theme = mode;
  localStorage.setItem('theme', mode);
  const icon = $('theme-icon');
  if (!icon) return;
  icon.innerHTML = mode === 'light'
    ? '<circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>'
    : '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>';
}
$('theme-toggle-btn').addEventListener('click', () => {
  const cur = document.documentElement.dataset.theme;
  applyTheme(cur === 'light' ? 'dark' : 'light');
});
// Apply saved theme on load
applyTheme(localStorage.getItem('theme') || 'dark');

/* ══════════════════════════════════════════
   SCREEN MANAGEMENT
══════════════════════════════════════════ */
function showScreen(id) {
  ['login-screen', 'setup-screen'].forEach(s => {
    const el = $(s);
    if (el) el.classList[id === s ? 'remove' : 'add']('hidden');
  });
}

/* ══════════════════════════════════════════
   MOBILE SIDEBAR
══════════════════════════════════════════ */
$('hamburger-btn').addEventListener('click', () => {
  $('sidebar').classList.add('open');
  $('sidebar-overlay').classList.add('open');
});
$('sidebar-overlay').addEventListener('click', closeSidebar);
function closeSidebar() {
  $('sidebar').classList.remove('open');
  $('sidebar-overlay').classList.remove('open');
}

/* ══════════════════════════════════════════
   CLOUDINARY UPLOAD
══════════════════════════════════════════ */
async function uploadToCloudinary(file, onProgress) {
  return new Promise((resolve, reject) => {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('upload_preset', CLD_PRESET);
    const xhr = new XMLHttpRequest();
    xhr.open('POST', CLD_URL);
    if (onProgress) xhr.upload.onprogress = e => {
      if (e.lengthComputable) onProgress(Math.round(e.loaded / e.total * 100));
    };
    xhr.onload = () => {
      if (xhr.status === 200) {
        try { resolve(JSON.parse(xhr.responseText).secure_url); }
        catch { reject(new Error('Parse error')); }
      } else { reject(new Error('Upload failed ' + xhr.status)); }
    };
    xhr.onerror = () => reject(new Error('Network error'));
    xhr.send(fd);
  });
}
async function uploadBlobToCloudinary(blob, filename) {
  return uploadToCloudinary(new File([blob], filename, { type: blob.type }));
}

/* ══════════════════════════════════════════
   AUTH
══════════════════════════════════════════ */
$('login-btn').addEventListener('click', () =>
  auth.signInWithPopup(new firebase.auth.GoogleAuthProvider())
    .catch(e => toast('Login failed: ' + e.message, 'e'))
);
$('guest-btn').addEventListener('click', () =>
  auth.signInAnonymously().catch(e => toast('Guest login failed: ' + e.message, 'e'))
);
$('signout-btn').addEventListener('click', async () => {
  const ok = await confirm2('Sign out', 'Are you sure you want to sign out?', '👋', 'Sign out');
  if (ok) auth.signOut();
});

auth.onAuthStateChanged(async user => {
  if (!user) {
    ME = null;
    $('app').classList.remove('visible');
    showScreen('login-screen');
    return;
  }
  isGuest = user.isAnonymous;
  try {
    const snap = await db.collection('users').doc(user.uid).get();
    if (!snap.exists || !snap.data().username) {
      showScreen('setup-screen');
      initSetup(user);
    } else {
      ME = { uid: user.uid, email: user.email || '', ...snap.data() };
      launchApp();
    }
  } catch (e) { console.error('Auth load error', e); showScreen('login-screen'); }
});

/* ══════════════════════════════════════════
   PROFILE SETUP
══════════════════════════════════════════ */
function initSetup(user) {
  let chosenPhoto = user.photoURL || '';
  let unameOK = false;
  let checkT = null;

  $('setup-av').src = chosenPhoto || dicebear(user.uid);
  if (user.isAnonymous) {
    $('setup-title').textContent = 'Set up guest profile 👤';
    $('setup-sub').textContent   = 'Pick a username. Your account auto-deletes in 7 days.';
    $('guest-setup-notice').classList.remove('hidden');
  }

  $('setup-upload-btn').addEventListener('click', () => $('setup-av-file').click());
  $('setup-av-file').addEventListener('change', async e => {
    const f = e.target.files[0]; if (!f) return;
    $('setup-av').src = URL.createObjectURL(f);
    try { chosenPhoto = await uploadToCloudinary(f); }
    catch { toast('Upload failed', 'e'); }
  });
  $('setup-url-btn').addEventListener('click', () => {
    urlCb = url => { chosenPhoto = url; $('setup-av').src = url; };
    $('url-in').value = '';
    showOv('url-ov');
  });

  $('setup-uname').addEventListener('input', function () {
    const v = this.value.trim().toLowerCase();
    clearTimeout(checkT);
    $('uname-msg').textContent = '';
    $('uname-msg').className = 'fmsg';
    this.classList.remove('err', 'ok');
    $('setup-confirm').disabled = true;
    unameOK = false;
    if (!v) return;
    if (v.length < 3)        { $('uname-msg').textContent = 'Min 3 characters.'; $('uname-msg').className = 'fmsg e'; return; }
    if (!/^[a-z0-9_]+$/.test(v)) { $('uname-msg').textContent = 'Letters, numbers, underscores only.'; $('uname-msg').className = 'fmsg e'; return; }
    $('uname-msg').textContent = 'Checking…';
    checkT = setTimeout(async () => {
      const taken = await db.collection('usernames').doc(v).get();
      if (taken.exists) {
        $('uname-msg').textContent = '⚠ Username taken.'; $('uname-msg').className = 'fmsg e';
        $('setup-uname').classList.add('err'); unameOK = false; $('setup-confirm').disabled = true;
      } else {
        $('uname-msg').textContent = '✓ Available!'; $('uname-msg').className = 'fmsg s';
        $('setup-uname').classList.add('ok'); unameOK = true; $('setup-confirm').disabled = false;
      }
    }, 500);
  });

  $('setup-confirm').addEventListener('click', async () => {
    if (!unameOK) return;
    const uname = $('setup-uname').value.trim().toLowerCase();
    const bio   = $('setup-bio').value.trim();
    $('setup-confirm').disabled = true;
    $('setup-confirm').textContent = 'Saving…';
    try {
      const deleteAt = user.isAnonymous ? new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) : null;
      await db.collection('usernames').doc(uname).set({ uid: user.uid });
      await db.collection('users').doc(user.uid).set({
        uid: user.uid, email: user.email || '', username: uname,
        bio: bio || '', photoURL: chosenPhoto,
        createdAt: tsNow(), lastSeen: tsNow(), friends: [],
        isGuest: user.isAnonymous, ...(deleteAt ? { deleteAt } : {}),
      });
      ME = { uid: user.uid, email: user.email || '', username: uname, bio: bio || '', photoURL: chosenPhoto, friends: [], isGuest: user.isAnonymous };
      launchApp();
    } catch (e) { toast('Error: ' + e.message, 'e'); $('setup-confirm').disabled = false; $('setup-confirm').textContent = "Let's go →"; }
  });
}

/* ══════════════════════════════════════════
   LAUNCH APP
══════════════════════════════════════════ */
function launchApp() {
  showScreen(null);
  $('app').classList.add('visible');
  updateTray();
  listenMyRooms();
  listenDMs();
  listenNotifications();
  listenIncomingCalls();
  syncPendingRequests();
  refreshLockedSidebar();
  db.collection('users').doc(ME.uid).update({ lastSeen: tsNow() }).catch(() => {});
}

function updateTray() {
  const av = ME.photoURL || dicebear(ME.uid);
  $('sb-my-av').src = av;
  $('tray-av').src  = av;
  $('tray-name').textContent = ME.username;
  $('guest-chip').classList[ME.isGuest ? 'remove' : 'add']('hidden');
}

/* ══════════════════════════════════════════
   SIDEBAR LISTENERS
══════════════════════════════════════════ */
function listenMyRooms() {
  if (roomUnsub) roomUnsub();
  roomUnsub = db.collection('rooms').where('memberIds', 'array-contains', ME.uid)
    .onSnapshot(snap => {
      const list   = $('room-list');
      list.innerHTML = '';
      const locked = getLockedData();
      snap.forEach(doc => {
        const r = doc.data();
        if (locked[doc.id]) return;
        const li = document.createElement('li');
        li.className   = 'sb-item' + (activeCh?.id === doc.id ? ' active' : '');
        li.dataset.id  = doc.id;
        const roleIcon = r.ownerId === ME.uid ? '👑' : (r.moderators || []).includes(ME.uid) ? '🛡' : '';
        li.innerHTML   = `<span class="item-icon">${r.isPrivate ? '🔒' : '#'}</span><span class="item-name" style="flex:1">${esc(r.name)} ${roleIcon}</span>`;
        li.addEventListener('click', () => { openRoom(doc.id, r); closeSidebar(); });
        addSb3dot(li, doc.id, 'room', r.name);
        list.appendChild(li);
      });
    }, err => console.warn('Rooms:', err));
}

function listenDMs() {
  if (dmUnsub) dmUnsub();
  dmUnsub = db.collection('dms').where('members', 'array-contains', ME.uid)
    .onSnapshot(snap => {
      const list    = $('dm-list');
      const reqList = $('req-list');
      const sec     = $('req-section');
      list.innerHTML = ''; reqList.innerHTML = '';
      const locked = getLockedData();
      let reqCount = 0;

      snap.forEach(doc => {
        const dm  = doc.data();
        if (dm.declined) return;
        const oid   = dm.members.find(m => m !== ME.uid);
        const other = dm.memberInfo?.[oid] || { username: 'User', photoURL: '' };
        const isReq = dm.isRequest === true;
        const iAmSender = dm.requestFrom === ME.uid;

        if (isReq && !iAmSender) {
          reqCount++;
          const li = document.createElement('li');
          li.className  = 'sb-item' + (activeCh?.id === doc.id ? ' active' : '');
          li.dataset.id = doc.id;
          li.innerHTML  = `<img class="item-av" src="${esc(other.photoURL || dicebear(oid))}" alt=""/><span class="item-name" style="flex:1">${esc(other.username)}</span><span class="item-badge">!</span>`;
          li.addEventListener('click', () => { openDM(doc.id, other.username, other.photoURL || '', oid, true); closeSidebar(); });
          reqList.appendChild(li);
        } else {
          if (locked[doc.id]) return;
          const li = document.createElement('li');
          li.className  = 'sb-item' + (activeCh?.id === doc.id ? ' active' : '');
          li.dataset.id = doc.id;
          li.innerHTML  = `<img class="item-av" src="${esc(other.photoURL || dicebear(oid))}" alt=""/><span class="item-name" style="flex:1">${esc(other.username)}${isReq && iAmSender ? ' <span style="font-size:10px;color:var(--mut)">(pending)</span>' : ''}</span>`;
          li.addEventListener('click', () => { openDM(doc.id, other.username, other.photoURL || '', oid, isReq && !iAmSender); closeSidebar(); });
          addSb3dot(li, doc.id, 'dm', other.username);
          list.appendChild(li);
        }
      });

      sec.style.display = reqCount > 0 ? 'flex' : 'none';
      const badge = $('req-count-badge');
      if (badge) badge.textContent = reqCount > 0 ? String(reqCount) : '';
    }, err => console.warn('DMs:', err));
}

/* ══════════════════════════════════════════
   NOTIFICATIONS
══════════════════════════════════════════ */
function listenNotifications() {
  if (notifUnsub) notifUnsub();
  notifUnsub = db.collection('notifications').where('toUid', '==', ME.uid)
    .onSnapshot(snap => {
      const unread = snap.docs.filter(d => d.data().read === false);
      const badge  = $('notif-badge');
      badge.classList[unread.length > 0 ? 'remove' : 'add']('hidden');
      if (unread.length > 0) badge.textContent = unread.length > 9 ? '9+' : String(unread.length);
      const sorted = [...snap.docs].sort((a, b) =>
        (b.data().createdAt?.toMillis?.() || 0) - (a.data().createdAt?.toMillis?.() || 0)
      );
      renderNotifications(sorted);
    }, err => console.warn('Notifs:', err.code, err.message));
}

function renderNotifications(docs) {
  const list  = $('notif-list');
  const unread = docs.filter(d => d.data().read === false);
  if (!unread.length) { list.innerHTML = '<div class="notif-empty">No new notifications.</div>'; return; }
  list.innerHTML = '';
  unread.forEach(doc => {
    const n = doc.data();
    const div = document.createElement('div');
    div.className = 'notif-item';
    const timeStr = n.createdAt?.toDate ? n.createdAt.toDate().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '';
    div.innerHTML = `
      <img class="notif-av" src="${esc(n.fromPhoto || dicebear(n.fromUid))}" alt=""/>
      <div class="notif-body">
        <div class="notif-text"><b>${esc(n.fromUsername)}</b> ${esc(n.message)}</div>
        ${timeStr ? `<div class="notif-time">${timeStr}</div>` : ''}
        ${n.type === 'friend_request' ? `<div class="notif-acts">
          <button class="notif-acc" data-nid="${doc.id}" data-fuid="${n.fromUid}" data-funame="${esc(n.fromUsername)}" data-fphoto="${esc(n.fromPhoto || '')}">✓ Accept</button>
          <button class="notif-dec" data-nid="${doc.id}" data-fuid="${n.fromUid}">✕ Decline</button>
        </div>` : ''}
      </div>`;
    list.appendChild(div);
  });

  // Delegate button handlers (avoid inline onclick)
  list.querySelectorAll('.notif-acc').forEach(btn => btn.addEventListener('click', () =>
    acceptFriendReq(btn.dataset.nid, btn.dataset.fuid, btn.dataset.funame, btn.dataset.fphoto)
  ));
  list.querySelectorAll('.notif-dec').forEach(btn => btn.addEventListener('click', () =>
    declineNotif(btn.dataset.nid, btn.dataset.fuid)
  ));
}

$('notif-btn').addEventListener('click', e => {
  e.stopPropagation();
  const panel = $('notif-panel');
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    db.collection('notifications').where('toUid', '==', ME.uid).where('read', '==', false).get()
      .then(snap => snap.forEach(d => d.ref.update({ read: true }))).catch(() => {});
  }
});

/* ══════════════════════════════════════════
   FRIEND REQUESTS
══════════════════════════════════════════ */
async function sendFriendRequest(toUid, toUsername, toPhoto) {
  if (isFriend(toUid))          { toast('Already friends!', 'i'); return; }
  if (pendingRequests.has(toUid)) { toast('Request already sent!', 'i'); return; }
  // If they already sent us one, auto-accept
  const theirReq = await db.collection('notifications')
    .where('fromUid', '==', toUid).where('toUid', '==', ME.uid)
    .where('type', '==', 'friend_request').where('read', '==', false).limit(1).get();
  if (!theirReq.empty) { await acceptFriendReq(theirReq.docs[0].id, toUid, toUsername, toPhoto); return; }
  await db.collection('notifications').add({
    type: 'friend_request', fromUid: ME.uid, fromUsername: ME.username, fromPhoto: ME.photoURL || '',
    toUid, message: 'sent you a friend request.', read: false, createdAt: tsNow(),
  });
  pendingRequests.add(toUid);
  localStorage.setItem('pendingReqs', JSON.stringify([...pendingRequests]));
  toast(`Friend request sent to ${toUsername}!`, 's');
  const q = $('u-search').value.trim().toLowerCase();
  if (q) searchUsers(q);
}

async function acceptFriendReq(notifId, fromUid, fromUsername, fromPhoto) {
  await db.collection('notifications').doc(notifId).update({ read: true });
  await db.collection('users').doc(ME.uid).update({ friends: FS.arrayUnion(fromUid) });
  await db.collection('users').doc(fromUid).update({ friends: FS.arrayUnion(ME.uid) });
  ME.friends = [...(ME.friends || []), fromUid];
  const dmSnap = await db.collection('dms').where('members', 'array-contains', ME.uid).where('requestFrom', '==', fromUid).get();
  dmSnap.forEach(d => d.ref.update({ isRequest: false }));
  await db.collection('notifications').add({
    type: 'friend_accepted', fromUid: ME.uid, fromUsername: ME.username, fromPhoto: ME.photoURL || '',
    toUid: fromUid, message: 'accepted your friend request! 🎉', read: false, createdAt: tsNow(),
  });
  $('notif-panel').classList.remove('open');
  toast(`You and ${fromUsername} are now friends!`, 's');
}

async function declineNotif(notifId, fromUid) {
  await db.collection('notifications').doc(notifId).update({ read: true });
  if (fromUid) pendingRequests.delete(fromUid);
  localStorage.setItem('pendingReqs', JSON.stringify([...pendingRequests]));
  toast('Request declined.', 'i');
}

function isFriend(uid) { return (ME.friends || []).includes(uid); }

async function syncPendingRequests() {
  try {
    const snap = await db.collection('notifications')
      .where('fromUid', '==', ME.uid).where('type', '==', 'friend_request').where('read', '==', false).get();
    const live = new Set(snap.docs.map(d => d.data().toUid));
    pendingRequests.forEach(uid => { if (!live.has(uid)) pendingRequests.delete(uid); });
    live.forEach(uid => pendingRequests.add(uid));
    localStorage.setItem('pendingReqs', JSON.stringify([...pendingRequests]));
  } catch (e) { console.warn('syncPending', e); }
}

/* ══════════════════════════════════════════
   OPEN ROOM / DM
══════════════════════════════════════════ */
function openRoom(id, roomData) {
  activeCh = { type: 'room', id, name: roomData.name, roomData };
  highlightSB();
  renderRoomFrame(id, roomData);
  subMessages('rooms/' + id + '/messages');
  renderMembers(roomData);
  startTypingListener('rooms/' + id);
}
function openDM(id, uname, photo, otherId, isRequest) {
  activeCh = { type: 'dm', id, name: uname, photoURL: photo, otherId, isRequest };
  highlightSB();
  renderDMFrame(id, uname, photo, otherId, isRequest);
  subMessages('dms/' + id + '/messages');
  startTypingListener('dms/' + id);
}
function highlightSB() {
  document.querySelectorAll('.sb-item').forEach(el =>
    el.classList.toggle('active', el.dataset.id === activeCh?.id)
  );
}

/* ══════════════════════════════════════════
   ROOM FRAME
══════════════════════════════════════════ */
function renderRoomFrame(id, room) {
  const isAdmin  = room.ownerId === ME.uid;
  const isMod2   = (room.moderators || []).includes(ME.uid);
  const canManage = isAdmin || isMod2;
  $('chat-main').innerHTML = `
    <div class="chat-hdr">
      <span class="hdr-icon">${room.isPrivate ? '🔒' : '#'}</span>
      <div class="hdr-info">
        <div class="hdr-name">${esc(room.name)} ${isAdmin ? '👑' : isMod2 ? '🛡' : ''}</div>
        <div class="hdr-desc">${room.isPrivate ? 'Private room' : 'Public room'} · ${(room.memberIds || []).length} members</div>
      </div>
      <div class="hdr-acts">
        <button class="room-code-chip" id="show-code-btn">
          <svg width="11" height="11" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
          ${esc(room.code)}
        </button>
        <button class="hdr-btn" id="members-toggle-btn" title="Members">
          <svg width="15" height="15" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        </button>
        ${canManage ? `<button class="hdr-btn" id="invite-btn" title="Invite">
          <svg width="15" height="15" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>
        </button>` : ''}
        ${isAdmin ? `<button class="hdr-btn red" id="delete-room-btn" title="Delete room">
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4h6v2"/></svg>
        </button>` : ''}
      </div>
    </div>
    ${renderSkeletons()}
    <div class="msgs-area" id="msgs-area">
      <div class="room-welcome">
        <div class="rw-icon">${room.isPrivate ? '🔒' : '🏠'}</div>
        <div class="rw-title">${room.isPrivate ? '' : '# '}${esc(room.name)}</div>
        <div class="rw-desc">${room.isPrivate ? 'Private room — invite only' : 'Public · Keywords: ' + esc((room.keywords || []).join(', '))}</div>
      </div>
    </div>
    <div class="typing-bar" id="typing-bar"></div>
    <div class="ai-bar hidden" id="ai-bar"></div>
    <div class="input-area"><div class="input-wrap">
      <div id="media-prev" class="media-prev" style="display:none"></div>
      <div class="input-top">
        <textarea id="msg-input" rows="1" placeholder="Message ${room.isPrivate ? '🔒' : '#'}${esc(room.name)}…"></textarea>
        <div class="input-acts">
          <button class="i-btn" id="emoji-btn" title="Emoji">😊</button>
          <button class="i-btn" id="attach-btn" title="Attach"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg></button>
          <button class="i-btn" id="voice-rec-btn" title="Voice"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 1 3 3v8a3 3 0 0 1-6 0V4a3 3 0 0 1 3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2M12 19v4M8 23h8"/></svg></button>
          <button class="send-btn" id="send-btn"><svg width="13" height="13" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5"><path d="M22 2 11 13M22 2 15 22 11 13 2 9l20-7z"/></svg></button>
        </div>
      </div>
    </div></div>
    <input type="file" id="file-input" accept="image/*,video/*" multiple class="hidden"/>`;

  bindInputEvents();
  $('show-code-btn').addEventListener('click', () => { $('room-code-display').textContent = room.code; showOv('roomcode-ov'); });
  $('members-toggle-btn').addEventListener('click', () => $('members-panel').classList.toggle('open'));
  if (canManage) $('invite-btn').addEventListener('click', () => openInviteModal(id, room));
  if (isAdmin)   $('delete-room-btn').addEventListener('click', () => deleteRoom(id, room.name));
}

/* ══════════════════════════════════════════
   DM FRAME
══════════════════════════════════════════ */
function renderDMFrame(id, uname, photo, otherId, isRequest) {
  const friend = isFriend(otherId);
  $('chat-main').innerHTML = `
    <div class="chat-hdr">
      <img class="hdr-av" src="${esc(photo || dicebear(otherId))}" alt=""/>
      <div class="hdr-info">
        <div class="hdr-name">${esc(uname)}</div>
        <div class="hdr-desc" id="hdr-status">${isRequest ? '⚠ Message request' : 'Private conversation'}</div>
      </div>
      <div class="hdr-acts">
        ${friend ? `
          <button class="hdr-btn" id="voice-call-btn" title="Voice call"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 12 19.79 19.79 0 0 1 1.63 3.4 2 2 0 0 1 3.6 1.22h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L7.91 8.96a16 16 0 0 0 6.07 6.07l.92-.92a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg></button>
          <button class="hdr-btn" id="video-call-btn" title="Video call"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="m23 7-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg></button>
        ` : ''}
      </div>
    </div>
    ${renderSkeletons()}
    <div class="msgs-area" id="msgs-area">
      <div class="room-welcome">
        <div class="rw-icon">👤</div>
        <div class="rw-title">${esc(uname)}</div>
        <div class="rw-desc">${isRequest ? 'This person is not your friend yet.' : 'Your private conversation with ' + esc(uname)}</div>
      </div>
      ${isRequest ? `<div class="req-banner">
        <div class="req-banner-txt">Message request from <b>${esc(uname)}</b>. You can reply without accepting.</div>
        <div class="req-banner-acts">
          <button class="req-accept-btn" id="accept-req-btn">✓ Accept &amp; Add Friend</button>
          <button class="req-decline-btn" id="decline-req-btn">✕ Decline</button>
        </div>
      </div>` : ''}
    </div>
    <div class="typing-bar" id="typing-bar"></div>
    <div class="ai-bar hidden" id="ai-bar"></div>
    <div class="input-area"><div class="input-wrap">
      <div id="media-prev" class="media-prev" style="display:none"></div>
      <div class="input-top">
        <textarea id="msg-input" rows="1" placeholder="Message ${esc(uname)}…"></textarea>
        <div class="input-acts">
          <button class="i-btn" id="emoji-btn" title="Emoji">😊</button>
          <button class="i-btn" id="attach-btn" title="Attach"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg></button>
          <button class="i-btn" id="voice-rec-btn" title="Voice"><svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 1 3 3v8a3 3 0 0 1-6 0V4a3 3 0 0 1 3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2M12 19v4M8 23h8"/></svg></button>
          <button class="send-btn" id="send-btn"><svg width="13" height="13" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5"><path d="M22 2 11 13M22 2 15 22 11 13 2 9l20-7z"/></svg></button>
        </div>
      </div>
    </div></div>
    <input type="file" id="file-input" accept="image/*,video/*" multiple class="hidden"/>`;

  bindInputEvents();
  if (friend) {
    $('voice-call-btn').addEventListener('click', () => startCall('voice'));
    $('video-call-btn').addEventListener('click', () => startCall('video'));
  }
  if (isRequest) {
    $('accept-req-btn').addEventListener('click', () => acceptMsgRequest(id, otherId, uname, photo || ''));
    $('decline-req-btn').addEventListener('click', () => declineMsgRequest(id));
  }

  // Show online status in header
  db.collection('users').doc(otherId).get().then(snap => {
    const u = snap.data();
    if (!u) return;
    const el = $('hdr-status');
    if (!el || isRequest) return;
    const lastSeen = u.lastSeen?.toDate?.();
    if (lastSeen) {
      const diff = Date.now() - lastSeen.getTime();
      if (diff < 5 * 60000) { el.innerHTML = '<span style="color:var(--on)">● Online</span>'; }
      else { el.textContent = 'Last seen ' + lastSeen.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
    }
  }).catch(() => {});
}

/** Render skeleton loading bars (hidden behind absolute overlap, removed when msgs load) */
function renderSkeletons() {
  return `<div id="skeleton-area" style="position:absolute;top:61px;left:0;right:0;padding:20px;display:flex;flex-direction:column;gap:14px;">
    ${[60, 45, 75, 40].map(w => `
      <div class="skeleton-msg">
        <div class="skel-av"></div>
        <div class="skel-body"><div class="skel-line" style="width:${w}px"></div><div class="skel-line" style="width:${w + 80}px;margin-top:4px"></div></div>
      </div>`).join('')}
  </div>`;
}

async function acceptMsgRequest(dmId, fromUid, fromUsername, fromPhoto) {
  await db.collection('dms').doc(dmId).update({ isRequest: false });
  await db.collection('users').doc(ME.uid).update({ friends: FS.arrayUnion(fromUid) });
  await db.collection('users').doc(fromUid).update({ friends: FS.arrayUnion(ME.uid) });
  ME.friends = [...(ME.friends || []), fromUid];
  await db.collection('notifications').add({
    type: 'friend_accepted', fromUid: ME.uid, fromUsername: ME.username, fromPhoto: ME.photoURL || '',
    toUid: fromUid, message: 'accepted your message request and added you as a friend! 🎉', read: false, createdAt: tsNow(),
  });
  openDM(dmId, fromUsername, fromPhoto, fromUid, false);
  toast('Friends now! You can call each other.', 's');
}
async function declineMsgRequest(dmId) {
  const ok = await confirm2('Decline Request', 'Decline and remove this message request?', '🗑', 'Decline', '');
  if (!ok) return;
  await db.collection('dms').doc(dmId).update({ isRequest: false, declined: true, declinedBy: ME.uid });
  activeCh = null;
  $('chat-main').innerHTML = `<div class="empty-view"><div class="empty-icon">💬</div><div class="empty-title">Welcome to Chatuwa</div><p class="empty-desc">Join a room or start a DM.</p></div>`;
  toast('Request declined.', 'i');
}

/* ══════════════════════════════════════════
   INPUT & TYPING
══════════════════════════════════════════ */
function bindInputEvents() {
  const ta = $('msg-input'); if (!ta) return;
  ta.addEventListener('input', () => {
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 100) + 'px';
    sendTyping();
  });
  ta.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMsg(); }
  });
  $('send-btn').addEventListener('click', sendMsg);
  $('attach-btn').addEventListener('click', () => $('file-input').click());
  $('file-input').addEventListener('change', onFileSelect);
  $('voice-rec-btn').addEventListener('click', toggleRec);
  $('emoji-btn').addEventListener('click', e => { e.stopPropagation(); toggleEmojiPicker(e.target); });
}

function sendTyping() {
  if (!activeCh || !ME) return;
  const path = activeCh.type === 'room' ? 'rooms/' + activeCh.id : 'dms/' + activeCh.id;
  db.collection(path).doc('__typing__').set({ [ME.uid]: { username: ME.username, t: Date.now() } }, { merge: true }).catch(() => {});
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() =>
    db.collection(path).doc('__typing__').set({ [ME.uid]: FS.delete() }, { merge: true }).catch(() => {}), 2500
  );
}
function startTypingListener(basePath) {
  if (typingUnsub) typingUnsub();
  typingUnsub = db.collection(basePath).doc('__typing__').onSnapshot(snap => {
    const data = snap.data() || {};
    const now  = Date.now();
    const typers = Object.entries(data)
      .filter(([uid, v]) => uid !== ME.uid && v?.t && (now - v.t) < 4000)
      .map(([, v]) => v.username);
    const bar = $('typing-bar'); if (!bar) return;
    if (!typers.length) { bar.innerHTML = ''; return; }
    bar.innerHTML = `<span class="t-dots"><span></span><span></span><span></span></span> <b>${esc(typers.join(', '))}</b> ${typers.length === 1 ? 'is' : 'are'} typing…`;
  }, () => {});
}

/* ══════════════════════════════════════════
   EMOJI PICKER
══════════════════════════════════════════ */
const EMOJIS = ['😀','😂','😍','🥺','😎','🔥','❤️','👍','😭','🎉','🙏','✨','💯','👀','🥳','😅','🤔','😤','💀','🫡','🤩','😇','🤣','😊','😋','🤗','🫶','💪','🙌','👏','🤌','💬','🏆','⭐','🌟','🚀','💡','🎯','🎮','🎵'];
const emojiGrid = $('emoji-grid');
EMOJIS.forEach(em => {
  const btn = document.createElement('button');
  btn.className = 'e-btn'; btn.textContent = em;
  btn.addEventListener('click', () => {
    const inp = $('msg-input'); if (inp) { inp.value += em; inp.focus(); }
    $('emoji-picker').classList.add('hidden');
  });
  emojiGrid.appendChild(btn);
});
function toggleEmojiPicker(anchor) {
  const picker = $('emoji-picker');
  if (!picker.classList.contains('hidden')) { picker.classList.add('hidden'); return; }
  const rect = anchor.getBoundingClientRect();
  picker.style.bottom = (window.innerHeight - rect.top + 6) + 'px';
  picker.style.right  = (window.innerWidth - rect.right) + 'px';
  picker.classList.remove('hidden');
}

/* ══════════════════════════════════════════
   MESSAGES — SUBSCRIBE
══════════════════════════════════════════ */
function subMessages(path) {
  if (msgUnsub) msgUnsub();
  const seen = new Set();
  // Remove skeleton after first batch
  let skeletonRemoved = false;
  function removeSkeleton() {
    if (skeletonRemoved) return;
    skeletonRemoved = true;
    const s = $('skeleton-area'); if (s) s.remove();
  }

  msgUnsub = db.collection(path).orderBy('createdAt', 'asc').limitToLast(80).onSnapshot(snap => {
    removeSkeleton();
    snap.docChanges().forEach(ch => {
      if (ch.type === 'added' && !seen.has(ch.doc.id)) {
        seen.add(ch.doc.id);
        renderMsg(ch.doc.data(), ch.doc.id, path);
      } else if (ch.type === 'modified') {
        const el = document.querySelector(`[data-msgid="${ch.doc.id}"]`);
        if (el) {
          const d = ch.doc.data();
          const bubble = el.querySelector('.bubble');
          if (bubble && d.type === 'text') {
            bubble.textContent = d.text;
            const em = el.querySelector('.msg-edited'); if (em) em.style.display = d.edited ? 'inline' : 'none';
          }
          // Update reactions
          renderReactions(el, d.reactions || {}, ch.doc.id, path);
        }
      } else if (ch.type === 'removed') {
        const el = document.querySelector(`[data-msgid="${ch.doc.id}"]`); if (el) el.remove();
      }
    });
    scrollBottom();
    // AI suggestions on last incoming text message
    const changes = snap.docChanges();
    const last = changes[changes.length - 1];
    if (last?.type === 'added' && last.doc.data().uid !== ME.uid && last.doc.data().type === 'text') {
      getAI(last.doc.data().text);
    }
    // Mark messages as read in DMs
    if (activeCh?.type === 'dm') markDMRead(path);
  });
}

/** Mark latest message seen */
async function markDMRead(path) {
  try {
    const snap = await db.collection(path).orderBy('createdAt', 'desc').limit(1).get();
    if (snap.empty) return;
    const msg = snap.docs[0];
    if (msg.data().uid !== ME.uid && !msg.data().seenBy?.[ME.uid]) {
      await msg.ref.update({ [`seenBy.${ME.uid}`]: true });
    }
  } catch {}
}

/* ══════════════════════════════════════════
   RENDER MESSAGE
══════════════════════════════════════════ */
function renderMsg(msg, msgId, msgPath) {
  const area = $('msgs-area'); if (!area) return;
  const isMe    = msg.uid === ME.uid;
  const av      = msg.photoURL || dicebear(msg.uid);
  const isAdmin = activeCh?.type === 'room' && activeCh?.roomData?.ownerId === ME.uid;
  const isMod2  = activeCh?.type === 'room' && (activeCh?.roomData?.moderators || []).includes(ME.uid);
  const canDelete = isMe || isAdmin || isMod2;
  const canEdit   = isMe && msg.type === 'text';

  // Build bubble content
  let body = '';
  if (msg.type === 'image' || msg.type === 'video') {
    const isImg = msg.type === 'image';
    if (msg.uploading) {
      body = `<div class="media-bub"><div style="width:200px;height:130px;background:var(--pan);border-radius:8px;position:relative;">
        <div class="upload-overlay"><div class="upload-spinner"></div><div class="upload-pct" id="upct-${msgId}">0%</div></div>
      </div></div>`;
    } else {
      body = isImg
        ? `<div class="media-bub"><img src="${esc(msg.url)}" alt="image" data-lb="${esc(msg.url)}"/></div>`
        : `<div class="media-bub"><video src="${esc(msg.url)}" controls playsinline></video></div>`;
    }
  } else if (msg.type === 'voice') {
    const aid = 'a' + Math.random().toString(36).slice(2);
    const bars = Array.from({ length: 28 }, (_, i) => {
      const h = Math.max(4, Math.round(Math.sin(i * 0.6) * 10 + 12));
      return `<div class="waveform-bar" style="height:${h}px" data-idx="${i}"></div>`;
    }).join('');
    body = `<div class="audio-bub">
      <button class="play-btn" data-aid="${aid}"><svg width="10" height="10" fill="white" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></button>
      <div class="waveform" data-aid="${aid}">${bars}</div>
      <span class="a-dur" id="d-${aid}">0:00</span>
      <audio id="${aid}" src="${esc(msg.url)}" style="display:none"></audio>
    </div>`;
  } else if (msg.type === 'room_invite') {
    body = `<div class="invite-bub">
      <div class="invite-title">📨 Room Invite</div>
      <div class="invite-name">${esc(msg.roomName)}</div>
      <div class="invite-code">Code: ${esc(msg.roomCode)}</div>
      <button class="invite-join-btn" data-code="${esc(msg.roomCode)}">Join Room</button>
    </div>`;
  } else {
    body = `<div class="bubble">${esc(msg.text)}<span class="msg-edited" style="display:${msg.edited ? 'inline' : 'none'}"> (edited)</span></div>`;
  }

  // Read receipt for own messages in DMs
  const receipt = (isMe && activeCh?.type === 'dm')
    ? `<div class="read-receipt${msg.seenBy && Object.keys(msg.seenBy).some(uid => uid !== ME.uid) ? ' seen' : ''}">
        ${msg.seenBy && Object.keys(msg.seenBy).some(uid => uid !== ME.uid) ? '✓✓ Seen' : '✓ Sent'}
      </div>` : '';

  const grp = document.createElement('div');
  grp.className   = 'msg-grp' + (isMe ? ' me' : '');
  grp.dataset.msgid = msgId;

  grp.innerHTML = `
    <img class="msg-av" src="${esc(av)}" alt="" onerror="this.style.visibility='hidden'" data-uid="${msg.uid}" data-uname="${esc(msg.username || '')}" data-uphoto="${esc(msg.photoURL || '')}"/>
    <div class="msg-col">
      <div class="msg-meta">
        <span class="msg-sender">${esc(isMe ? 'You' : msg.username || 'User')}</span>
        <span class="msg-time">${fmtTime(msg.createdAt)}</span>
      </div>
      ${body}
      <div class="reactions-row" id="rx-${msgId}"></div>
      ${receipt}
    </div>`;

  // 3-dot options button — correct side per Discord convention
  if (canDelete || canEdit) {
    const optBtn = document.createElement('button');
    optBtn.className = 'msg-opts-btn';
    optBtn.title = 'Options';
    // Vertical 3-dots
    optBtn.innerHTML = '<svg width="3" height="14" viewBox="0 0 3 14" fill="currentColor"><circle cx="1.5" cy="1.5" r="1.5"/><circle cx="1.5" cy="7" r="1.5"/><circle cx="1.5" cy="12.5" r="1.5"/></svg>';
    optBtn.addEventListener('click', ev => showMsgCtx(ev, msgId, msgPath, msg.type, msg.text || '', isMe, canDelete, canEdit, msg.reactions || {}));

    if (isMe) {
      // Own message: dots on the LEFT of the bubble (which renders on right due to flex-reverse)
      const col = grp.querySelector('.msg-col');
      grp.insertBefore(optBtn, col);
    } else {
      // Other's message: dots on the RIGHT
      grp.appendChild(optBtn);
    }
  }

  area.appendChild(grp);

  // Wire click events after insertion
  const lbImgs = grp.querySelectorAll('[data-lb]');
  lbImgs.forEach(img => img.addEventListener('click', () => openLB(img.dataset.lb)));
  const avEl = grp.querySelector('.msg-av');
  if (avEl) avEl.addEventListener('click', e => showProfilePopup(e, avEl.dataset.uid, avEl.dataset.uname, avEl.dataset.uphoto));
  const playBtns = grp.querySelectorAll('.play-btn[data-aid]');
  playBtns.forEach(btn => btn.addEventListener('click', () => playAudio(btn.dataset.aid, grp)));
  const waveforms = grp.querySelectorAll('.waveform[data-aid]');
  waveforms.forEach(wf => wf.addEventListener('click', ev => scrubAudio(ev, wf.dataset.aid)));
  const joinBtns = grp.querySelectorAll('.invite-join-btn[data-code]');
  joinBtns.forEach(btn => btn.addEventListener('click', () => joinRoomByCode(btn.dataset.code)));

  // Initial reactions
  renderReactions(grp, msg.reactions || {}, msgId, msgPath);
}

/* ══════════════════════════════════════════
   REACTIONS
══════════════════════════════════════════ */
const REACTION_EMOJIS = ['❤️', '👍', '😂', '🔥', '😮', '😢', '👏', '🎉'];

function renderReactions(grp, reactions, msgId, msgPath) {
  const row = grp.querySelector(`#rx-${msgId}`); if (!row) return;
  row.innerHTML = '';
  Object.entries(reactions).forEach(([emoji, uids]) => {
    if (!uids.length) return;
    const chip = document.createElement('div');
    chip.className = 'reaction-chip' + (uids.includes(ME.uid) ? ' mine' : '');
    chip.innerHTML = `${emoji} <span class="rc">${uids.length}</span>`;
    chip.title = `${uids.length} reaction${uids.length > 1 ? 's' : ''}`;
    chip.addEventListener('click', () => toggleReaction(emoji, msgId, msgPath, reactions));
    row.appendChild(chip);
  });
}

async function toggleReaction(emoji, msgId, msgPath, currentReactions) {
  const uids = currentReactions[emoji] || [];
  const hasIt = uids.includes(ME.uid);
  const updatedUids = hasIt ? uids.filter(u => u !== ME.uid) : [...uids, ME.uid];
  await db.collection(msgPath).doc(msgId).update({ [`reactions.${emoji}`]: updatedUids }).catch(() => {});
}

/* ══════════════════════════════════════════
   MSG CONTEXT MENU (3-dot)
══════════════════════════════════════════ */
function showMsgCtx(e, msgId, msgPath, msgType, msgText, isMe, canDelete, canEdit, reactions) {
  e.stopPropagation();
  document.querySelectorAll('.msg-ctx').forEach(m => m.remove());
  const menu = document.createElement('div');
  menu.className = 'msg-ctx';

  // Quick reaction row
  const rxRow = document.createElement('div');
  rxRow.className = 'ctx-reactions';
  REACTION_EMOJIS.forEach(em => {
    const btn = document.createElement('button');
    btn.className = 'ctx-emoji-btn'; btn.textContent = em;
    btn.title = em;
    btn.addEventListener('click', () => { menu.remove(); toggleReaction(em, msgId, msgPath, reactions); });
    rxRow.appendChild(btn);
  });
  menu.appendChild(rxRow);

  if (canEdit) {
    const ei = document.createElement('div'); ei.className = 'ctx-item'; ei.innerHTML = '✏️ Edit';
    ei.addEventListener('click', () => { menu.remove(); startEditMsg(msgId, msgPath, msgText); });
    menu.appendChild(ei);
  }
  if (msgType === 'text' && msgText) {
    const ci = document.createElement('div'); ci.className = 'ctx-item'; ci.innerHTML = '📋 Copy';
    ci.addEventListener('click', () => { navigator.clipboard.writeText(msgText); toast('Copied!', 'i'); menu.remove(); });
    menu.appendChild(ci);
  }
  if (canDelete) {
    const di = document.createElement('div'); di.className = 'ctx-item danger'; di.innerHTML = isMe ? '🗑 Delete' : '🗑 Delete (Admin)';
    di.addEventListener('click', () => { menu.remove(); deleteMsg(msgId, msgPath); });
    menu.appendChild(di);
  }

  const rect = e.currentTarget.getBoundingClientRect();
  menu.style.top  = Math.min(rect.bottom + 4, window.innerHeight - 260) + 'px';
  menu.style.left = Math.min(rect.left, window.innerWidth - 180) + 'px';
  document.body.appendChild(menu);
}

async function deleteMsg(msgId, msgPath) {
  const ok = await confirm2('Delete Message', 'This message will be permanently deleted for everyone.', '🗑', 'Delete', '');
  if (!ok) return;
  try { await db.collection(msgPath).doc(msgId).delete(); toast('Message deleted.', 'i'); }
  catch (e) { toast('Delete failed: ' + e.message, 'e'); }
}
function startEditMsg(msgId, msgPath, currentText) {
  editingMsgId   = msgId;
  editingMsgPath = msgPath;
  $('edit-msg-text').value = currentText;
  showOv('edit-msg-ov');
  $('edit-msg-text').focus();
}
$('edit-msg-confirm').addEventListener('click', async () => {
  if (!editingMsgId) return;
  const newText = $('edit-msg-text').value.trim();
  if (!newText) { toast('Cannot be empty.', 'e'); return; }
  try {
    await db.collection(editingMsgPath).doc(editingMsgId).update({ text: newText, edited: true });
    hideOv('edit-msg-ov'); editingMsgId = null; editingMsgPath = null; toast('Message updated.', 's');
  } catch (e) { toast('Edit failed: ' + e.message, 'e'); }
});

/* ══════════════════════════════════════════
   PROFILE POPUP (on avatar click)
══════════════════════════════════════════ */
async function showProfilePopup(e, uid, username, photoURL) {
  e.stopPropagation();
  document.querySelectorAll('.profile-popup').forEach(p => p.remove());
  if (uid === ME.uid) return;
  const isPending = pendingRequests.has(uid);
  const friend    = isFriend(uid);
  const popup = document.createElement('div');
  popup.className = 'profile-popup';
  popup.innerHTML = `
    <div class="pp-banner"><img class="pp-av" src="${esc(photoURL || dicebear(uid))}" alt=""/></div>
    <div class="pp-body">
      <div class="pp-name">${esc(username)}</div>
      <div class="pp-bio" id="ppbio-${uid}">Loading…</div>
      <div class="pp-status" id="ppstatus-${uid}"></div>
      <div class="pp-acts">
        <button class="pp-btn msg" data-uid="${uid}" data-uname="${esc(username)}" data-uphoto="${esc(photoURL || '')}">💬 Message</button>
        ${friend ? `<button class="pp-btn friend" disabled>✓ Friends</button>`
          : isPending ? `<button class="pp-btn pending" disabled>⏳ Requested</button>`
          : `<button class="pp-btn add" data-uid="${uid}" data-uname="${esc(username)}" data-uphoto="${esc(photoURL || '')}">➕ Add</button>`}
      </div>
    </div>`;
  const rect = e.target.getBoundingClientRect();
  popup.style.top  = Math.min(rect.top, window.innerHeight - 290) + 'px';
  popup.style.left = Math.min(rect.right + 8, window.innerWidth - 240) + 'px';
  document.body.appendChild(popup);

  popup.querySelector('.pp-btn.msg')?.addEventListener('click', async btn => {
    document.querySelectorAll('.profile-popup').forEach(p => p.remove());
    await startDMwith(uid, username, photoURL || '');
  });
  popup.querySelector('.pp-btn.add')?.addEventListener('click', () => {
    document.querySelectorAll('.profile-popup').forEach(p => p.remove());
    sendFriendRequest(uid, username, photoURL || '');
  });

  // Load bio + online status
  try {
    const snap = await db.collection('users').doc(uid).get();
    const u = snap.data() || {};
    const bioEl = document.getElementById('ppbio-' + uid); if (bioEl) bioEl.textContent = u.bio || 'No bio yet.';
    const stEl  = document.getElementById('ppstatus-' + uid); if (stEl) {
      const ls = u.lastSeen?.toDate?.();
      if (ls) {
        const diff = Date.now() - ls.getTime();
        stEl.innerHTML = diff < 5 * 60000
          ? '<span style="color:var(--on)">● Online</span>'
          : `<span style="color:var(--mut)">Last seen ${ls.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>`;
      }
    }
  } catch {}
}

/* ══════════════════════════════════════════
   SEND MESSAGE
══════════════════════════════════════════ */
async function sendMsg() {
  if (!activeCh) return;
  const input = $('msg-input');
  const text  = input?.value.trim() || '';
  if (!text && !pendingFiles.length && !voiceBlob) return;
  input.value = '';
  if (input) input.style.height = 'auto';
  hideAI();

  const path = activeCh.type === 'room'
    ? 'rooms/' + activeCh.id + '/messages'
    : 'dms/'   + activeCh.id + '/messages';
  const base = { uid: ME.uid, username: ME.username, photoURL: ME.photoURL || '', createdAt: tsNow(), reactions: {} };

  if (text) await db.collection(path).add({ ...base, type: 'text', text, edited: false });

  const filesToSend = [...pendingFiles];
  const voiceToSend = voiceBlob;
  pendingFiles = []; voiceBlob = null;
  const pb = $('media-prev');
  if (pb) { pb.innerHTML = ''; pb.style.display = 'none'; }

  // Upload files with progress placeholder
  for (const pf of filesToSend) {
    const placeholderRef = await db.collection(path).add({ ...base, type: pf.type, url: '', uploading: true, text: '' });
    const msgId = placeholderRef.id;
    try {
      const url = await uploadToCloudinary(pf.file, pct => {
        const pctEl = document.getElementById('upct-' + msgId);
        if (pctEl) pctEl.textContent = pct + '%';
      });
      await placeholderRef.update({ url, uploading: false });
    } catch (e) { toast('Upload failed: ' + e.message, 'e'); await placeholderRef.delete(); }
  }

  if (voiceToSend) {
    try {
      const url = await uploadBlobToCloudinary(voiceToSend, 'voice_' + Date.now() + '.webm');
      await db.collection(path).add({ ...base, type: 'voice', url, text: '' });
    } catch (e) { toast('Voice upload failed: ' + e.message, 'e'); }
  }

  if (activeCh.type === 'dm') db.collection('dms').doc(activeCh.id).update({ lastMessage: tsNow() }).catch(() => {});
}

/* ══════════════════════════════════════════
   FILE ATTACH
══════════════════════════════════════════ */
function onFileSelect(e) {
  [...e.target.files].forEach(f => {
    const type = f.type.startsWith('video/') ? 'video' : 'image';
    const url  = URL.createObjectURL(f);
    pendingFiles.push({ file: f, type, url });
    addThumb(f, type, url);
  });
  e.target.value = '';
}
function addThumb(file, type, url) {
  const bar = $('media-prev'); if (!bar) return;
  bar.style.display = 'flex';
  const d = document.createElement('div'); d.className = 'm-thumb';
  d.innerHTML = (type === 'video' ? `<video src="${url}"></video>` : `<img src="${url}" alt=""/>`) +
    `<button class="rm-th" data-url="${url}">✕</button>`;
  d.querySelector('.rm-th').addEventListener('click', btn => {
    pendingFiles = pendingFiles.filter(p => p.url !== url);
    d.remove();
    if (bar && !bar.children.length) bar.style.display = 'none';
  });
  bar.appendChild(d);
}

/* ══════════════════════════════════════════
   VOICE RECORDING
══════════════════════════════════════════ */
async function toggleRec() {
  const btn = $('voice-rec-btn');
  if (isRec) { mediaRec.stop(); isRec = false; btn.classList.remove('rec'); return; }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    recChunks = [];
    mediaRec = new MediaRecorder(stream);
    mediaRec.ondataavailable = e => { if (e.data.size > 0) recChunks.push(e.data); };
    mediaRec.onstop = () => {
      voiceBlob = new Blob(recChunks, { type: 'audio/webm' });
      stream.getTracks().forEach(t => t.stop());
      const bar = $('media-prev'); if (!bar) return;
      bar.style.display = 'flex';
      const old = bar.querySelector('.voice-prev'); if (old) old.remove();
      const vp = document.createElement('div'); vp.className = 'voice-prev';
      vp.innerHTML = `🎙 Voice (${fmtSz(voiceBlob.size)}) <button id="cancel-voice-btn"><svg width="10" height="10" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5"><path d="M18 6 6 18M6 6l12 12"/></svg></button>`;
      vp.querySelector('#cancel-voice-btn').addEventListener('click', () => {
        voiceBlob = null; vp.remove();
        if (bar && !bar.children.length) bar.style.display = 'none';
      });
      bar.appendChild(vp);
    };
    mediaRec.start(); isRec = true; btn.classList.add('rec');
  } catch { toast('Mic access denied.', 'e'); }
}

/* ══════════════════════════════════════════
   AUDIO PLAYBACK (waveform scrub)
══════════════════════════════════════════ */
function playAudio(aid, grp) {
  const a = $(aid); if (!a) return;
  const dEl = $('d-' + aid);
  if (a.paused) {
    a.play();
    a.ontimeupdate = () => {
      if (!a.duration) return;
      const pct = a.currentTime / a.duration;
      const bars = grp.querySelectorAll(`.waveform[data-aid="${aid}"] .waveform-bar`);
      bars.forEach((b, i) => b.classList.toggle('played', i / bars.length < pct));
      const s = Math.floor(a.currentTime);
      if (dEl) dEl.textContent = Math.floor(s / 60) + ':' + (s % 60).toString().padStart(2, '0');
    };
    a.onended = () => {
      const bars = grp.querySelectorAll(`.waveform[data-aid="${aid}"] .waveform-bar`);
      bars.forEach(b => b.classList.remove('played'));
    };
  } else { a.pause(); }
}
function scrubAudio(ev, aid) {
  const a = $(aid); if (!a || !a.duration) return;
  const wf  = ev.currentTarget;
  const rect = wf.getBoundingClientRect();
  const pct  = (ev.clientX - rect.left) / rect.width;
  a.currentTime = pct * a.duration;
}

/* ══════════════════════════════════════════
   AI REPLY SUGGESTIONS
══════════════════════════════════════════ */
function hideAI() { const b = $('ai-bar'); if (b) b.classList.add('hidden'); }
async function getAI(lastMsg) {
  const bar = $('ai-bar'); if (!bar) return;
  bar.classList.remove('hidden');
  bar.innerHTML = '<div class="ai-lbl">✦ AI Replies</div><span style="font-size:12px;color:var(--mut)">Thinking…</span>';
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514', max_tokens: 100,
        system: 'Return ONLY a JSON array of exactly 3 short chat reply strings (max 8 words each). No markdown, no explanation.',
        messages: [{ role: 'user', content: 'Reply suggestions for: "' + lastMsg.replace(/"/g, '\\"') + '"' }],
      }),
    });
    const data = await res.json();
    const raw  = data?.content?.[0]?.text?.trim() || '[]';
    let chips; try { chips = JSON.parse(raw.replace(/```json|```/g, '').trim()); } catch { chips = []; }
    if (!chips.length) { bar.classList.add('hidden'); return; }
    bar.innerHTML = '<div class="ai-lbl">✦ AI Replies</div>' +
      chips.map(c => `<button class="ai-chip" data-v="${esc(c)}">${esc(c)}</button>`).join('');
    bar.querySelectorAll('.ai-chip').forEach(btn =>
      btn.addEventListener('click', () => { const i = $('msg-input'); if (i) { i.value = btn.dataset.v; i.focus(); } })
    );
  } catch { bar.classList.add('hidden'); }
}

/* ══════════════════════════════════════════
   PROFILE EDIT
══════════════════════════════════════════ */
function openMyProfile() {
  $('profile-title').textContent = 'My Profile';
  $('prof-av').src   = ME.photoURL || dicebear(ME.uid);
  $('prof-name').textContent = ME.username;
  $('prof-bio').textContent  = ME.bio || 'No bio yet.';
  $('edit-bio').value        = ME.bio || '';
  $('prof-edit').style.display = 'flex';
  $('prof-acts').style.display = 'flex';
  showOv('profile-ov');
}
[$('sb-my-av'), $('tray-av')].forEach(el => el?.addEventListener('click', openMyProfile));
$('edit-upload-btn').addEventListener('click', () => $('edit-av-file').click());
$('edit-av-file').addEventListener('change', async e => {
  const f = e.target.files[0]; if (!f) return;
  $('prof-av').src = URL.createObjectURL(f);
  try { const url = await uploadToCloudinary(f); ME.photoURL = url; $('prof-av').src = url; toast('Photo updated — save profile to apply.', 'i'); }
  catch { toast('Upload failed', 'e'); }
});
$('edit-url-btn').addEventListener('click', () => { urlCb = url => { ME.photoURL = url; $('prof-av').src = url; }; $('url-in').value = ''; showOv('url-ov'); });
$('save-prof-btn').addEventListener('click', async () => {
  const bio = $('edit-bio').value.trim();
  ME.bio = bio;
  try {
    await db.collection('users').doc(ME.uid).update({ bio, photoURL: ME.photoURL || '' });
    updateTray(); hideOv('profile-ov'); toast('Profile saved!', 's');
  } catch (e) { toast('Save failed: ' + e.message, 'e'); }
});

/* ══════════════════════════════════════════
   USER SEARCH & DM
══════════════════════════════════════════ */
$('new-dm-btn').addEventListener('click', () => { $('u-search').value = ''; $('u-search-results').innerHTML = ''; showOv('search-ov'); $('u-search').focus(); });
$('u-search').addEventListener('input', function () {
  clearTimeout(userSearchTimer);
  const q = this.value.trim().toLowerCase();
  if (!q) { $('u-search-results').innerHTML = ''; return; }
  userSearchTimer = setTimeout(() => searchUsers(q), 350);
});
async function searchUsers(q) {
  const res = $('u-search-results');
  res.innerHTML = '<div style="font-size:12.5px;color:var(--mut);padding:10px">Searching…</div>';
  const snap = await db.collection('users').where('username', '>=', q).where('username', '<=', q + '\uf8ff').limit(10).get();
  res.innerHTML = '';
  if (snap.empty) { res.innerHTML = '<div style="font-size:12.5px;color:var(--mut);padding:10px">No users found.</div>'; return; }
  const myDoc = await db.collection('users').doc(ME.uid).get(); ME.friends = myDoc.data().friends || [];
  snap.forEach(doc => {
    const u = doc.data(); if (u.uid === ME.uid) return;
    const friend    = isFriend(u.uid);
    const isPending = pendingRequests.has(u.uid);
    const d = document.createElement('div'); d.className = 'u-result';
    d.innerHTML = `<img class="u-av" src="${esc(u.photoURL || dicebear(u.uid))}" alt=""/>
      <div class="u-info"><div class="u-name">${esc(u.username)}</div><div class="u-bio">${esc(u.bio || 'No bio')}</div></div>
      ${friend ? `<button class="u-action-btn friend" data-uid="${u.uid}" data-uname="${esc(u.username)}" data-uphoto="${esc(u.photoURL || '')}">💬 Message</button>`
        : isPending ? `<button class="u-action-btn pending" disabled>⏳ Requested</button>`
        : `<button class="u-action-btn add" data-uid="${u.uid}" data-uname="${esc(u.username)}" data-uphoto="${esc(u.photoURL || '')}">➕ Add Friend</button>`}`;
    d.querySelector('.u-action-btn.friend')?.addEventListener('click', btn => startDMwith(btn.dataset.uid, btn.dataset.uname, btn.dataset.uphoto));
    d.querySelector('.u-action-btn.add')?.addEventListener('click', btn => sendFriendRequest(btn.dataset.uid, btn.dataset.uname, btn.dataset.uphoto));
    res.appendChild(d);
  });
}
async function startDMwith(otherId, uname, photo) {
  hideOv('search-ov');
  const snap = await db.collection('dms').where('members', 'array-contains', ME.uid).get();
  let existing = null;
  snap.forEach(doc => { if (doc.data().members.includes(otherId) && !doc.data().declined) existing = doc; });
  let dmId;
  if (existing) { dmId = existing.id; }
  else {
    const isFr = isFriend(otherId);
    const ref = await db.collection('dms').add({
      members: [ME.uid, otherId],
      memberInfo: { [ME.uid]: { username: ME.username, photoURL: ME.photoURL || '' }, [otherId]: { username: uname, photoURL: photo } },
      isRequest: !isFr, requestFrom: !isFr ? ME.uid : null, lastMessage: tsNow(),
    });
    dmId = ref.id;
    if (!isFr) {
      await db.collection('notifications').add({
        type: 'message_request', fromUid: ME.uid, fromUsername: ME.username, fromPhoto: ME.photoURL || '',
        toUid: otherId, message: 'sent you a message request.', read: false, createdAt: tsNow(),
      });
    }
  }
  const isReq = !isFriend(otherId) && existing?.data()?.isRequest;
  openDM(dmId, uname, photo, otherId, isReq || false);
}

/* ══════════════════════════════════════════
   ROOMS
══════════════════════════════════════════ */
$('create-room-btn').addEventListener('click', () => {
  $('room-name-in').value = ''; $('room-keywords-in').value = '';
  isPrivateRoom = false; $('private-toggle').classList.remove('on');
  showOv('room-ov'); $('room-name-in').focus();
});
$('private-toggle').addEventListener('click', function () {
  isPrivateRoom = !isPrivateRoom; this.classList.toggle('on', isPrivateRoom);
});
$('room-confirm-btn').addEventListener('click', async () => {
  const name = $('room-name-in').value.trim().toLowerCase().replace(/\s+/g, '-');
  if (!name) { toast('Enter a room name.', 'e'); return; }
  const kw = ($('room-keywords-in').value.trim() || '').split(',').map(k => k.trim().toLowerCase()).filter(Boolean);
  kw.push(name);
  const code = genCode();
  const ref = await db.collection('rooms').add({ name, keywords: kw, isPrivate: isPrivateRoom, code, ownerId: ME.uid, moderators: [], memberIds: [ME.uid], members: { [ME.uid]: { username: ME.username, photoURL: ME.photoURL || '', role: 'admin' } }, createdAt: tsNow() });
  hideOv('room-ov');
  openRoom(ref.id, { name, keywords: kw, isPrivate: isPrivateRoom, code, ownerId: ME.uid, moderators: [], memberIds: [ME.uid], members: {} });
  toast(`Room #${name} created!`, 's');
});

$('find-room-btn').addEventListener('click', () => { $('room-search-in').value = ''; $('room-search-results').innerHTML = ''; $('room-search-results').classList.remove('open'); showOv('search-rooms-ov'); $('room-search-in').focus(); });
$('join-by-code-btn').addEventListener('click', () => { hideOv('search-rooms-ov'); $('join-code-in').value = ''; showOv('join-room-ov'); });
$('room-search-in').addEventListener('input', function () {
  clearTimeout(roomSearchTimer);
  const q = this.value.trim().toLowerCase();
  const res = $('room-search-results');
  if (!q) { res.classList.remove('open'); res.innerHTML = ''; return; }
  roomSearchTimer = setTimeout(() => searchRooms(q), 300);
});

async function searchRooms(q) {
  const res = $('room-search-results');
  res.innerHTML = '<div style="padding:8px 12px;font-size:12px;color:var(--mut)">Searching…</div>';
  res.classList.add('open');
  const snap    = await db.collection('rooms').where('isPrivate', '==', false).where('keywords', 'array-contains', q).limit(8).get();
  const snapAll = await db.collection('rooms').where('isPrivate', '==', false).limit(30).get();
  const results = new Map();
  snap.forEach(d => results.set(d.id, d));
  snapAll.forEach(d => { if (results.has(d.id)) return; const kws = d.data().keywords || []; if (kws.some(k => k.startsWith(q))) results.set(d.id, d); });
  res.innerHTML = '';
  if (!results.size) { res.innerHTML = '<div style="padding:8px 12px;font-size:12px;color:var(--mut)">No rooms found.</div>'; return; }
  results.forEach(doc => {
    const r = doc.data(); const alreadyMember = (r.memberIds || []).includes(ME.uid);
    const div = document.createElement('div'); div.className = 'room-sug';
    div.innerHTML = `<div class="room-sug-name">#${esc(r.name)} ${alreadyMember ? '<span style="font-size:10px;color:var(--on)">✓ Joined</span>' : ''}</div><div class="room-sug-meta">${(r.memberIds || []).length} members · ${esc((r.keywords || []).join(', '))}</div>`;
    div.addEventListener('click', () => {
      res.classList.remove('open');
      if (alreadyMember) { hideOv('search-rooms-ov'); openRoom(doc.id, r); return; }
      showJoinConfirm(doc, r);
    });
    res.appendChild(div);
  });
}
function showJoinConfirm(doc, r) {
  hideOv('search-rooms-ov');
  $('jr-room-name').textContent = '#' + r.name;
  $('jr-meta').textContent = `${(r.memberIds || []).length} members · ${r.isPrivate ? 'Private' : 'Public'} room`;
  joinPendingDoc = { doc, r }; showOv('join-confirm-ov');
}
$('jr-cancel').addEventListener('click', () => { hideOv('join-confirm-ov'); joinPendingDoc = null; });
$('jr-join').addEventListener('click', async () => {
  if (!joinPendingDoc) return;
  const { doc, r } = joinPendingDoc; joinPendingDoc = null; hideOv('join-confirm-ov');
  await db.collection('rooms').doc(doc.id).update({ memberIds: FS.arrayUnion(ME.uid), ['members.' + ME.uid]: { username: ME.username, photoURL: ME.photoURL || '', role: 'member' } });
  openRoom(doc.id, { ...r, memberIds: [...(r.memberIds || []), ME.uid] });
  toast(`Joined #${r.name}!`, 's');
});
$('join-confirm-btn').addEventListener('click', async () => {
  const code = $('join-code-in').value.trim().toUpperCase();
  if (!code) { toast('Enter a code.', 'e'); return; }
  await joinRoomByCode(code);
});
async function joinRoomByCode(code) {
  const snap = await db.collection('rooms').where('code', '==', code).limit(1).get();
  if (snap.empty) { toast('No room with that code.', 'e'); return; }
  const doc = snap.docs[0]; const r = doc.data();
  if ((r.memberIds || []).includes(ME.uid)) { hideOv('join-room-ov'); openRoom(doc.id, r); return; }
  showJoinConfirm(doc, r); hideOv('join-room-ov');
}

function openInviteModal(roomId, room) {
  const uname = prompt('Enter username to invite:'); if (!uname) return;
  inviteUserToRoom(uname, roomId, room);
}
async function inviteUserToRoom(uname, roomId, room) {
  const snap = await db.collection('users').where('username', '==', uname.trim().toLowerCase()).limit(1).get();
  if (snap.empty) { toast('User not found.', 'e'); return; }
  const other = snap.docs[0].data();
  const dmSnap = await db.collection('dms').where('members', 'array-contains', ME.uid).get();
  let dmId = null; dmSnap.forEach(d => { if (d.data().members.includes(other.uid)) dmId = d.id; });
  if (!dmId) {
    const ref = await db.collection('dms').add({ members: [ME.uid, other.uid], memberInfo: { [ME.uid]: { username: ME.username, photoURL: ME.photoURL || '' }, [other.uid]: { username: other.username, photoURL: other.photoURL || '' } }, isRequest: false, lastMessage: tsNow() });
    dmId = ref.id;
  }
  await db.collection('dms').doc(dmId).collection('messages').add({ uid: ME.uid, username: ME.username, photoURL: ME.photoURL || '', type: 'room_invite', roomName: room.name, roomCode: room.code, text: '', createdAt: tsNow() });
  toast('Invite sent to ' + other.username + '!', 's');
}
$('copy-code-btn').addEventListener('click', () => navigator.clipboard.writeText($('room-code-display').textContent).then(() => toast('Code copied!', 's')));
async function deleteRoom(roomId, roomName) {
  const ok = await confirm2(`Delete #${roomName}?`, 'This will permanently delete the room and all messages.', '⚠️', 'Delete Room', '');
  if (!ok) return;
  try {
    const msgs = await db.collection('rooms').doc(roomId).collection('messages').get();
    const batch = db.batch(); msgs.forEach(d => batch.delete(d.ref)); batch.delete(db.collection('rooms').doc(roomId));
    await batch.commit();
    activeCh = null;
    $('chat-main').innerHTML = `<div class="empty-view"><div class="empty-icon">🗑</div><div class="empty-title">Room Deleted</div><p class="empty-desc">The room has been permanently deleted.</p></div>`;
    toast(`#${roomName} deleted.`, 'i');
  } catch (e) { toast('Could not delete: ' + e.message, 'e'); }
}

/* ══════════════════════════════════════════
   MEMBERS PANEL
══════════════════════════════════════════ */
function renderMembers(room) {
  const panel   = $('members-panel');
  const members = room.members || {};
  panel.innerHTML = `<div class="members-hdr">Members (${(room.memberIds || []).length})</div>`;
  Object.entries(members).forEach(([uid, m]) => {
    const role = uid === room.ownerId ? 'admin' : (room.moderators || []).includes(uid) ? 'mod' : '';
    const div = document.createElement('div'); div.className = 'member-item';
    div.innerHTML = `<img class="member-av" src="${esc(m.photoURL || dicebear(uid))}" alt=""/>
      <span class="member-name">${esc(m.username || 'User')}</span>
      ${role ? `<span class="member-role ${role}">${role === 'admin' ? 'Admin' : 'Mod'}</span>` : ''}`;
    if (uid !== ME.uid) div.addEventListener('click', e => showProfilePopup(e, uid, m.username || 'User', m.photoURL || ''));
    panel.appendChild(div);
  });
}

/* ══════════════════════════════════════════
   SIDEBAR FILTER
══════════════════════════════════════════ */
$('sb-search-input').addEventListener('input', function () {
  const q = this.value.toLowerCase();
  document.querySelectorAll('.sb-item').forEach(el =>
    el.style.display = el.textContent.toLowerCase().includes(q) ? '' : 'none'
  );
});

/* ══════════════════════════════════════════
   URL MODAL
══════════════════════════════════════════ */
$('url-confirm-btn').addEventListener('click', () => {
  const url = $('url-in').value.trim(); if (!url) return;
  if (urlCb) { urlCb(url); urlCb = null; } hideOv('url-ov');
});

/* ══════════════════════════════════════════
   LIGHTBOX
══════════════════════════════════════════ */
function openLB(url) { $('lb-img').src = url; $('lightbox').classList.add('open'); }
$('lightbox').addEventListener('click', () => $('lightbox').classList.remove('open'));

/* ══════════════════════════════════════════
   UTILITY
══════════════════════════════════════════ */
function scrollBottom() {
  const a = $('msgs-area'); if (a) { a.scrollTop = a.scrollHeight; }
}

/* ══════════════════════════════════════════
   WEBRTC CALLS
══════════════════════════════════════════ */
const ICE_SERVERS = { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun1.l.google.com:19302' }] };

async function startCall(type) {
  if (!activeCh || activeCh.type !== 'dm') return;
  if (!isFriend(activeCh.otherId)) { toast('You can only call friends.', 'e'); return; }
  callType = type; isCaller = true;
  localStream = await navigator.mediaDevices.getUserMedia(type === 'video' ? { audio: true, video: true } : { audio: true, video: false })
    .catch(e => { toast('Cannot access: ' + e.message, 'e'); return null; });
  if (!localStream) return;
  pc = new RTCPeerConnection(ICE_SERVERS);
  localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
  if (type === 'video') {
    $('local-vid').srcObject = localStream; $('video-wrap').classList.add('on');
    pc.ontrack = e => { $('remote-vid').srcObject = e.streams[0]; };
  } else { showCallOv(activeCh.name, activeCh.photoURL, 'Calling…'); }
  const ref = await db.collection('calls').add({ caller: ME.uid, callerName: ME.username, callerPhoto: ME.photoURL || '', callee: activeCh.otherId, type, status: 'ringing', dmId: activeCh.id, offer: null, answer: null, createdAt: tsNow() });
  callDocId = ref.id;
  pc.onicecandidate = e => { if (e.candidate) db.collection('calls').doc(callDocId).collection('callerCandidates').add(e.candidate.toJSON()); };
  const offer = await pc.createOffer(); await pc.setLocalDescription(offer);
  await db.collection('calls').doc(callDocId).update({ offer: { type: offer.type, sdp: offer.sdp } });
  callUnsub = db.collection('calls').doc(callDocId).onSnapshot(async snap => {
    const d = snap.data(); if (!d) return;
    if (d.status === 'declined') { endCall(); return; }
    if (d.answer && pc.signalingState === 'have-local-offer') {
      await pc.setRemoteDescription(new RTCSessionDescription(d.answer));
      $('call-stat').textContent = 'Connected';
      db.collection('calls').doc(callDocId).collection('calleeCandidates').get().then(s => s.forEach(c => pc.addIceCandidate(new RTCIceCandidate(c.data()))));
    }
    if (d.status === 'ended') endCall();
  });
}
function listenIncomingCalls() {
  db.collection('calls').where('callee', '==', ME.uid).where('status', '==', 'ringing').onSnapshot(snap => {
    snap.docChanges().forEach(ch => { if (ch.type === 'added') showCallToast(ch.doc.id, ch.doc.data()); });
  });
}
function showCallToast(docId, data) {
  callDocId = docId; callType = data.type; isCaller = false;
  $('toast-av').src = data.callerPhoto || dicebear(data.caller);
  $('toast-name').textContent = data.callerName;
  $('toast-type').textContent = (data.type === 'video' ? '📹 Video' : '📞 Voice') + ' call…';
  $('call-toast').classList.add('on');
}
$('toast-acc').addEventListener('click', async () => {
  $('call-toast').classList.remove('on');
  const snap = await db.collection('calls').doc(callDocId).get();
  const data = snap.data(); if (!data || data.status !== 'ringing') return;
  localStream = await navigator.mediaDevices.getUserMedia(callType === 'video' ? { audio: true, video: true } : { audio: true, video: false }).catch(e => { toast('Cannot access: ' + e.message, 'e'); return null; });
  if (!localStream) return;
  pc = new RTCPeerConnection(ICE_SERVERS); localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
  if (callType === 'video') { $('local-vid').srcObject = localStream; $('video-wrap').classList.add('on'); pc.ontrack = e => { $('remote-vid').srcObject = e.streams[0]; }; }
  else { showCallOv(data.callerName, data.callerPhoto, 'Connected'); }
  pc.onicecandidate = e => { if (e.candidate) db.collection('calls').doc(callDocId).collection('calleeCandidates').add(e.candidate.toJSON()); };
  await pc.setRemoteDescription(new RTCSessionDescription(data.offer));
  const answer = await pc.createAnswer(); await pc.setLocalDescription(answer);
  await db.collection('calls').doc(callDocId).update({ answer: { type: answer.type, sdp: answer.sdp }, status: 'active' });
  db.collection('calls').doc(callDocId).collection('callerCandidates').get().then(s => s.forEach(c => pc.addIceCandidate(new RTCIceCandidate(c.data()))));
  callUnsub = db.collection('calls').doc(callDocId).onSnapshot(snap => { if (snap.data()?.status === 'ended') endCall(); });
});
$('toast-dec').addEventListener('click', async () => {
  $('call-toast').classList.remove('on');
  if (callDocId) await db.collection('calls').doc(callDocId).update({ status: 'declined' });
  callDocId = null;
});
function showCallOv(name, photo, status) {
  $('call-av').src = photo || dicebear('default');
  $('call-name').textContent = name;
  $('call-stat').textContent = status;
  $('call-overlay').classList.add('on');
}
function endCall() {
  if (localStream) localStream.getTracks().forEach(t => t.stop());
  if (pc) { pc.close(); pc = null; } localStream = null;
  if (callUnsub) { callUnsub(); callUnsub = null; }
  if (callDocId) { db.collection('calls').doc(callDocId).update({ status: 'ended' }).catch(() => {}); callDocId = null; }
  $('call-overlay').classList.remove('on'); $('video-wrap').classList.remove('on');
  $('remote-vid').srcObject = null; $('local-vid').srcObject = null;
  isMuted = false; isCamOff = false;
}
$('end-call-btn').addEventListener('click', endCall);
$('end-vid-btn').addEventListener('click', endCall);
$('mute-btn').addEventListener('click', function () {
  if (!localStream) return; isMuted = !isMuted;
  localStream.getAudioTracks().forEach(t => t.enabled = !isMuted);
  [$('mute-btn'), $('vid-mute-btn')].forEach(b => { if (b) b.style.opacity = isMuted ? '.45' : '1'; });
});
$('vid-mute-btn').addEventListener('click', function () {
  if (!localStream) return; isMuted = !isMuted;
  localStream.getAudioTracks().forEach(t => t.enabled = !isMuted);
  [$('mute-btn'), $('vid-mute-btn')].forEach(b => { if (b) b.style.opacity = isMuted ? '.45' : '1'; });
});
$('vid-cam-btn').addEventListener('click', function () {
  if (!localStream) return; isCamOff = !isCamOff;
  localStream.getVideoTracks().forEach(t => t.enabled = !isCamOff);
  this.style.opacity = isCamOff ? '.45' : '1';
});

/* ══════════════════════════════════════════
   SIDEBAR 3-DOT MENU
══════════════════════════════════════════ */
function addSb3dot(li, chatId, chatType, chatName) {
  const btn = document.createElement('button');
  btn.className = 'sb-3dot'; btn.title = 'Options';
  btn.innerHTML = '<svg width="13" height="13" fill="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="5" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="12" cy="19" r="2"/></svg>';
  btn.addEventListener('click', e => {
    e.stopPropagation();
    document.querySelectorAll('.sb-ctx').forEach(m => m.remove());
    const isLocked = isChatlocked(chatId);
    const menu = document.createElement('div'); menu.className = 'sb-ctx';

    const lockItem = document.createElement('div'); lockItem.className = 'sb-ctx-item lock';
    lockItem.innerHTML = isLocked ? '🔓 Move to All Chats' : '🔐 Move to Locked';
    lockItem.addEventListener('click', () => { menu.remove(); isLocked ? unlockChat(chatId, chatName) : promptLockChat(chatId, chatName, chatType); });
    menu.appendChild(lockItem);

    if (chatType === 'dm') {
      const leaveItem = document.createElement('div'); leaveItem.className = 'sb-ctx-item danger';
      leaveItem.innerHTML = '🚪 Leave Chat';
      leaveItem.addEventListener('click', async () => {
        menu.remove();
        const ok = await confirm2('Leave Chat', `Leave your chat with ${chatName}?`, '🚪', 'Leave');
        if (ok) {
          await db.collection('dms').doc(chatId).update({ [`hidden.${ME.uid}`]: true });
          if (activeCh?.id === chatId) {
            activeCh = null;
            $('chat-main').innerHTML = '<div class="empty-view"><div class="empty-icon">💬</div><div class="empty-title">Welcome to Chatuwa</div><p class="empty-desc">Join a room or start a DM.</p></div>';
          }
        }
      });
      menu.appendChild(leaveItem);
    }

    const rect = btn.getBoundingClientRect();
    menu.style.top  = rect.bottom + 4 + 'px';
    menu.style.left = Math.min(rect.left, window.innerWidth - 170) + 'px';
    document.body.appendChild(menu);
  });
  li.appendChild(btn);
}

/* ══════════════════════════════════════════
   LOCKED CHATS — Firestore-backed
   Stored in user's Firestore doc for cross-device sync
══════════════════════════════════════════ */
function getLockedData() {
  try { return JSON.parse(localStorage.getItem('lockedChats') || '{}'); }
  catch { return {}; }
}
function saveLockedData(data) {
  localStorage.setItem('lockedChats', JSON.stringify(data));
  // Also sync to Firestore for cross-device support
  if (ME?.uid) {
    db.collection('users').doc(ME.uid).update({ lockedChats: data }).catch(() => {});
  }
}
function isChatlocked(chatId) { return !!getLockedData()[chatId]; }

// On launch, merge Firestore locked data into localStorage
async function syncLockedChats() {
  try {
    const snap = await db.collection('users').doc(ME.uid).get();
    const remoteData = snap.data()?.lockedChats || {};
    const localData  = getLockedData();
    const merged = { ...remoteData, ...localData };
    saveLockedData(merged);
    refreshLockedSidebar();
  } catch {}
}

/* PIN MODAL LOGIC */
$('pin-ov').querySelectorAll('.pin-key[data-key]').forEach(btn =>
  btn.addEventListener('click', () => handlePinKey(btn.dataset.key))
);

function handlePinKey(k) {
  if (k === 'del') { pinEntry = pinEntry.slice(0, -1); }
  else if (pinEntry.length < 4) { pinEntry += k; }
  updatePinDots();
  $('pin-error').textContent = '';
  if (pinEntry.length === 4) {
    setTimeout(() => {
      if (pinMode === 'set')    doSetPin();
      else if (pinMode === 'verify') doVerifyPin();
    }, 120);
  }
}
function updatePinDots() {
  for (let i = 0; i < 4; i++) {
    const d = $('pd' + i);
    if (d) d.classList.toggle('filled', i < pinEntry.length);
  }
}
function promptLockChat(chatId, chatName, chatType) {
  pinMode = 'set'; pinChatId = chatId; pinChatName = chatName; pinChatType = chatType; pinEntry = '';
  $('pin-title').textContent = 'Lock "' + chatName + '"';
  $('pin-sub').textContent   = 'Set a 4-digit PIN to protect this chat.';
  $('pin-error').textContent = '';
  updatePinDots(); showOv('pin-ov');
}
function doSetPin() {
  const data = getLockedData();
  data[pinChatId] = { name: pinChatName, type: pinChatType, pin: hashPin(pinEntry) };
  saveLockedData(data);
  hideOv('pin-ov'); pinEntry = '';
  refreshLockedSidebar();
  toast(`"${pinChatName}" moved to Locked Chats 🔐`, 's');
}
function unlockChat(chatId, chatName) {
  pinMode = 'verify'; pinChatId = chatId; pinEntry = '';
  $('pin-title').textContent = 'Unlock "' + chatName + '"';
  $('pin-sub').textContent   = 'Enter your PIN to unlock this chat.';
  $('pin-error').textContent = '';
  updatePinDots();
  pinVerifyCallback = () => {
    const data = getLockedData(); delete data[chatId]; saveLockedData(data);
    refreshLockedSidebar(); hideOv('pin-ov'); pinEntry = '';
    toast(`"${chatName}" moved back to All Chats`, 's');
  };
  showOv('pin-ov');
}
function openLockedChat(chatId, chatName) {
  pinMode = 'verify'; pinChatId = chatId; pinEntry = '';
  $('pin-title').textContent = '🔐 ' + chatName;
  $('pin-sub').textContent   = 'Enter your PIN to open this chat.';
  $('pin-error').textContent = '';
  updatePinDots();
  const data = getLockedData(); const info = data[chatId];
  pinVerifyCallback = async () => {
    hideOv('pin-ov'); pinEntry = '';
    if (info.type === 'dm') {
      const snap = await db.collection('dms').doc(chatId).get(); if (!snap.exists) return;
      const dm  = snap.data(); const oid = dm.members.find(m => m !== ME.uid);
      const other = dm.memberInfo?.[oid] || { username: chatName, photoURL: '' };
      openDM(chatId, other.username, other.photoURL || '', oid, false);
    } else {
      const snap = await db.collection('rooms').doc(chatId).get(); if (!snap.exists) return;
      openRoom(chatId, snap.data());
    }
  };
  showOv('pin-ov');
}
function doVerifyPin() {
  const data = getLockedData(); const info = data[pinChatId];
  if (!info) { hideOv('pin-ov'); pinEntry = ''; return; }
  if (hashPin(pinEntry) === info.pin) {
    if (pinVerifyCallback) pinVerifyCallback();
    pinVerifyCallback = null;
  } else {
    pinEntry = ''; updatePinDots();
    $('pin-error').textContent = 'Wrong PIN. Try again.';
  }
}
function refreshLockedSidebar() {
  const data = getLockedData(); const keys = Object.keys(data);
  const sec  = $('locked-section'); const list = $('locked-list');
  sec.style.display = keys.length > 0 ? 'flex' : 'none';
  list.innerHTML = '';
  keys.forEach(chatId => {
    const info = data[chatId];
    const li = document.createElement('li');
    li.className  = 'sb-item' + (activeCh?.id === chatId ? ' active' : '');
    li.dataset.id = chatId;
    // Never reveal chat name until unlocked — show only lock icon + blurred placeholder
    li.innerHTML = `<span class="item-lockbadge">🔐</span><span class="locked-item-name">••••••••••</span>`;
    li.addEventListener('click', () => openLockedChat(chatId, info.name));
    list.appendChild(li);
  });
}

/* ══════════════════════════════════════════
   GLOBAL CLICK DISMISSALS
══════════════════════════════════════════ */
document.addEventListener('click', e => {
  // Close message context menu
  if (!e.target.closest('.msg-ctx') && !e.target.closest('.msg-opts-btn'))
    document.querySelectorAll('.msg-ctx').forEach(m => m.remove());
  // Close profile popup
  if (!e.target.closest('.profile-popup') && !e.target.closest('.msg-av') && !e.target.closest('.member-item'))
    document.querySelectorAll('.profile-popup').forEach(p => p.remove());
  // Close notif panel
  const np = $('notif-panel'), nb = $('notif-btn');
  if (np?.classList.contains('open') && !np.contains(e.target) && !nb.contains(e.target))
    np.classList.remove('open');
  // Close sidebar context menus
  if (!e.target.closest('.sb-ctx') && !e.target.closest('.sb-3dot'))
    document.querySelectorAll('.sb-ctx').forEach(m => m.remove());
  // Close emoji picker
  if (!e.target.closest('.emoji-picker') && !e.target.closest('#emoji-btn'))
    $('emoji-picker')?.classList.add('hidden');
});

/* ══════════════════════════════════════════
   REMAINING PART OF APP.JS (COMPLETE)
   Paste this at the very end of your current app.js
══════════════════════════════════════════ */

// ── REACTIONS ──
const REACTION_EMOJIS = ['❤️', '👍', '😂', '🔥', '😮', '😢', '👏', '🎉'];

function renderReactions(grp, reactions, msgId, msgPath) {
  const row = grp.querySelector(`#rx-${msgId}`);
  if (!row) return;
  row.innerHTML = '';
  Object.entries(reactions || {}).forEach(([emoji, uids]) => {
    if (!uids || !uids.length) return;
    const chip = document.createElement('div');
    chip.className = `reaction-chip${uids.includes(ME.uid) ? ' mine' : ''}`;
    chip.innerHTML = `${emoji} <span class="rc">${uids.length}</span>`;
    chip.addEventListener('click', () => toggleReaction(emoji, msgId, msgPath, reactions));
    row.appendChild(chip);
  });
}

async function toggleReaction(emoji, msgId, msgPath, currentReactions) {
  const uids = currentReactions[emoji] || [];
  const hasIt = uids.includes(ME.uid);
  const updated = hasIt 
    ? uids.filter(u => u !== ME.uid) 
    : [...uids, ME.uid];
  
  try {
    await db.collection(msgPath).doc(msgId).update({ 
      [`reactions.${emoji}`]: updated 
    });
  } catch (e) {
    toast('Failed to update reaction', 'e');
  }
}

// ── MESSAGE CONTEXT MENU (Vertical 3-dots like Discord) ──
function showMsgCtx(e, msgId, msgPath, msgType, msgText, isMe, canDelete, canEdit) {
  e.stopPropagation();
  document.querySelectorAll('.msg-ctx').forEach(m => m.remove());

  const menu = document.createElement('div');
  menu.className = 'msg-ctx';

  // Quick reactions row
  const rxRow = document.createElement('div');
  rxRow.className = 'ctx-reactions';
  REACTION_EMOJIS.forEach(em => {
    const btn = document.createElement('button');
    btn.className = 'ctx-emoji-btn';
    btn.textContent = em;
    btn.addEventListener('click', () => {
      menu.remove();
      toggleReaction(em, msgId, msgPath, {});
    });
    rxRow.appendChild(btn);
  });
  menu.appendChild(rxRow);

  if (canEdit) {
    const editItem = document.createElement('div');
    editItem.className = 'ctx-item';
    editItem.innerHTML = '✏️ Edit';
    editItem.addEventListener('click', () => {
      menu.remove();
      startEditMsg(msgId, msgPath, msgText);
    });
    menu.appendChild(editItem);
  }

  if (msgType === 'text' && msgText) {
    const copyItem = document.createElement('div');
    copyItem.className = 'ctx-item';
    copyItem.innerHTML = '📋 Copy';
    copyItem.addEventListener('click', () => {
      navigator.clipboard.writeText(msgText);
      toast('Copied to clipboard', 'i');
      menu.remove();
    });
    menu.appendChild(copyItem);
  }

  if (canDelete) {
    const delItem = document.createElement('div');
    delItem.className = 'ctx-item danger';
    delItem.innerHTML = isMe ? '🗑 Delete' : '🗑 Delete (Admin)';
    delItem.addEventListener('click', () => {
      menu.remove();
      deleteMsg(msgId, msgPath);
    });
    menu.appendChild(delItem);
  }

  const rect = e.currentTarget.getBoundingClientRect();
  menu.style.position = 'fixed';
  menu.style.top = Math.min(rect.bottom + 6, window.innerHeight - 220) + 'px';
  menu.style.left = Math.min(rect.left - 10, window.innerWidth - 190) + 'px';
  document.body.appendChild(menu);
}

// ── LOCKED CHATS (Firestore synced) ──
function getLockedData() {
  try { return JSON.parse(localStorage.getItem('lockedChats') || '{}'); }
  catch { return {}; }
}

function saveLockedData(data) {
  localStorage.setItem('lockedChats', JSON.stringify(data));
  if (ME?.uid) {
    db.collection('users').doc(ME.uid).update({ lockedChats: data }).catch(() => {});
  }
}

function isChatlocked(chatId) {
  return !!getLockedData()[chatId];
}

async function syncLockedChats() {
  if (!ME?.uid) return;
  try {
    const snap = await db.collection('users').doc(ME.uid).get();
    const remote = snap.data()?.lockedChats || {};
    const local = getLockedData();
    const merged = { ...remote, ...local };
    saveLockedData(merged);
    refreshLockedSidebar();
  } catch (e) { console.warn('Locked chats sync failed', e); }
}

// PIN handling
function updatePinDots() {
  for (let i = 0; i < 4; i++) {
    const d = $('pd' + i);
    if (d) d.classList.toggle('filled', i < pinEntry.length);
  }
}

function promptLockChat(chatId, chatName, chatType) {
  pinMode = 'set';
  pinChatId = chatId;
  pinChatName = chatName;
  pinChatType = chatType;
  pinEntry = '';
  $('pin-title').textContent = `Lock "${chatName}"`;
  $('pin-sub').textContent = 'Set a 4-digit PIN to protect this chat.';
  $('pin-error').textContent = '';
  updatePinDots();
  showOv('pin-ov');
}

function doSetPin() {
  const data = getLockedData();
  data[pinChatId] = { name: pinChatName, type: pinChatType, pin: hashPin(pinEntry) };
  saveLockedData(data);
  hideOv('pin-ov');
  pinEntry = '';
  refreshLockedSidebar();
  toast(`"${pinChatName}" moved to Locked Chats 🔐`, 's');
}

function unlockChat(chatId, chatName) {
  pinMode = 'verify';
  pinChatId = chatId;
  pinEntry = '';
  $('pin-title').textContent = `Unlock "${chatName}"`;
  $('pin-sub').textContent = 'Enter your PIN to unlock this chat.';
  $('pin-error').textContent = '';
  updatePinDots();
  pinVerifyCallback = () => {
    const data = getLockedData();
    delete data[chatId];
    saveLockedData(data);
    refreshLockedSidebar();
    hideOv('pin-ov');
    pinEntry = '';
    toast(`"${chatName}" moved back to All Chats`, 's');
  };
  showOv('pin-ov');
}

function openLockedChat(chatId, chatName) {
  pinMode = 'verify';
  pinChatId = chatId;
  pinEntry = '';
  $('pin-title').textContent = `🔐 ${chatName}`;
  $('pin-sub').textContent = 'Enter your PIN to open this chat.';
  $('pin-error').textContent = '';
  updatePinDots();
  const data = getLockedData();
  const info = data[chatId];
  pinVerifyCallback = async () => {
    hideOv('pin-ov');
    pinEntry = '';
    if (info.type === 'dm') {
      const snap = await db.collection('dms').doc(chatId).get();
      if (!snap.exists) return;
      const dm = snap.data();
      const oid = dm.members.find(m => m !== ME.uid);
      const other = dm.memberInfo?.[oid] || { username: chatName, photoURL: '' };
      openDM(chatId, other.username, other.photoURL || '', oid, false);
    } else {
      const snap = await db.collection('rooms').doc(chatId).get();
      if (!snap.exists) return;
      openRoom(chatId, snap.data());
    }
  };
  showOv('pin-ov');
}

function doVerifyPin() {
  const data = getLockedData();
  const info = data[pinChatId];
  if (!info) {
    hideOv('pin-ov');
    pinEntry = '';
    return;
  }
  if (hashPin(pinEntry) === info.pin) {
    if (pinVerifyCallback) pinVerifyCallback();
    pinVerifyCallback = null;
  } else {
    pinEntry = '';
    updatePinDots();
    $('pin-error').textContent = 'Wrong PIN. Try again.';
  }
}

function refreshLockedSidebar() {
  const data = getLockedData();
  const keys = Object.keys(data);
  const sec = $('locked-section');
  const list = $('locked-list');
  sec.style.display = keys.length > 0 ? 'flex' : 'none';
  list.innerHTML = '';
  keys.forEach(chatId => {
    const info = data[chatId];
    const li = document.createElement('li');
    li.className = 'sb-item' + (activeCh?.id === chatId ? ' active' : '');
    li.dataset.id = chatId;
    li.innerHTML = `<span class="item-lockbadge">🔐</span><span class="locked-item-name">Locked Chat</span>`;
    li.addEventListener('click', () => openLockedChat(chatId, info.name));
    list.appendChild(li);
  });
}

// ── GLOBAL CLICK HANDLERS ──
document.addEventListener('click', e => {
  // Close message context
  if (!e.target.closest('.msg-ctx') && !e.target.closest('.msg-opts-btn')) {
    document.querySelectorAll('.msg-ctx').forEach(m => m.remove());
  }
  // Close profile popup
  if (!e.target.closest('.profile-popup') && !e.target.closest('.msg-av') && !e.target.closest('.member-item')) {
    document.querySelectorAll('.profile-popup').forEach(p => p.remove());
  }
  // Close emoji picker
  if (!e.target.closest('#emoji-picker') && !e.target.closest('#emoji-btn')) {
    const picker = $('emoji-picker');
    if (picker) picker.classList.add('hidden');
  }
});

// Final initialization
async function initializeApp() {
  await syncLockedChats();
  console.log('%c✅ Chatuwa fully initialized', 'color:#f0c040; font-weight:bold');
}

// Call after launchApp
const originalLaunch = launchApp;
launchApp = async function() {
  originalLaunch();
  await initializeApp();
};

console.log('%cChatuwa app.js loaded successfully', 'color:#f0c040');