const $ = (id) => document.getElementById(id)
let ws
const logEl = $('log')

function line(cls, html) {
  const d = document.createElement('div')
  d.className = 'line ' + cls
  d.innerHTML = html
  logEl.appendChild(d)
  logEl.scrollTop = logEl.scrollHeight
}

function connect() {
  if (ws) ws.close()
  const name = encodeURIComponent($('name').value || 'anon')
  const room = encodeURIComponent($('room').value || 'lobby')
  const proto = location.protocol === 'https:' ? 'wss' : 'ws'
  ws = new WebSocket(`${proto}://${location.host}/ws?name=${name}&room=${room}`)
  $('status').textContent = 'connecting…'
  ws.onopen = () => {
    $('status').textContent = 'online'
    $('status').classList.add('on')
    line('presence', `connected to #${$('room').value}`)
    refreshStats()
  }
  ws.onclose = () => {
    $('status').textContent = 'offline'
    $('status').classList.remove('on')
  }
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data)
    if (m.type === 'chat') line('chat', `<b>${esc(m.from)}</b> ${esc(m.text)}`)
    else if (m.type === 'presence') line('presence', `${esc(m.from)} ${esc(m.text)} · ${m.members} online`)
  }
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}

async function refreshStats() {
  try {
    const s = await fetch('/v1/stats').then((r) => r.json())
    $('stats').textContent = `clients=${s.clients} rooms=${JSON.stringify(s.rooms)}`
  } catch {}
}

$('connect').onclick = connect
$('form').onsubmit = (e) => {
  e.preventDefault()
  if (!ws || ws.readyState !== 1) return
  const text = $('msg').value.trim()
  if (!text) return
  ws.send(JSON.stringify({ type: 'chat', text }))
  $('msg').value = ''
}
setInterval(refreshStats, 5000)
