const $ = (id) => document.getElementById(id)
const logEl = $('log')
let ws = null
let typingTimer = null

function line(html, cls = '') {
  const d = document.createElement('div')
  d.className = 'line ' + cls
  d.innerHTML = html
  logEl.appendChild(d)
  logEl.scrollTop = logEl.scrollHeight
}

function setStatus(on) {
  $('status').textContent = on ? 'online' : 'offline'
  $('status').classList.toggle('on', on)
}

function connect() {
  if (ws) ws.close()
  const name = encodeURIComponent($('name').value || 'anon')
  const room = encodeURIComponent($('room').value || 'lobby')
  const token = encodeURIComponent($('token').value || '')
  const proto = location.protocol === 'https:' ? 'wss' : 'ws'
  const url = `${proto}://${location.host}/ws?name=${name}&room=${room}&token=${token}`
  ws = new WebSocket(url)
  ws.onopen = () => {
    setStatus(true)
    line('<span class="meta">connected</span>', 'sys')
  }
  ws.onclose = () => {
    setStatus(false)
    line('<span class="meta">disconnected</span>', 'sys')
  }
  ws.onmessage = (ev) => {
    let msg
    try {
      msg = JSON.parse(ev.data)
    } catch {
      return
    }
    if (msg.type === 'chat') {
      line(`<span class="from">${esc(msg.from)}</span> <span class="meta">#${esc(msg.room)}</span> ${esc(msg.text)}`, 'chat')
    } else if (msg.type === 'presence') {
      line(`<span class="meta">${esc(msg.from)} ${esc(msg.text)} · ${msg.members} in #${esc(msg.room)}</span>`, 'sys')
    } else if (msg.type === 'roster') {
      line(`<span class="meta">roster: ${esc(msg.text || '')}</span>`, 'sys')
    } else if (msg.type === 'typing') {
      $('typing').textContent = `${msg.from} is typing…`
      clearTimeout(typingTimer)
      typingTimer = setTimeout(() => { $('typing').textContent = '' }, 1200)
    } else if (msg.type === 'error') {
      line(`<span class="meta">error: ${esc(msg.text)}</span>`, 'sys')
    }
  }
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

$('connect').onclick = connect
$('switch').onclick = () => {
  if (!ws || ws.readyState !== 1) return connect()
  ws.send(JSON.stringify({
    type: 'join',
    room: $('room').value || 'lobby',
    token: $('token').value || '',
  }))
}

$('form').onsubmit = (e) => {
  e.preventDefault()
  const text = $('msg').value.trim()
  if (!text || !ws || ws.readyState !== 1) return
  ws.send(JSON.stringify({ type: 'chat', text }))
  $('msg').value = ''
}

$('msg').addEventListener('input', () => {
  if (!ws || ws.readyState !== 1) return
  ws.send(JSON.stringify({ type: 'typing' }))
})

fetch('/v1/stats').then((r) => r.json()).then((s) => {
  line(`<span class="meta">hub clients=${s.clients || 0}</span>`, 'sys')
}).catch(() => {})
