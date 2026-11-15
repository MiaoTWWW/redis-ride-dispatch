// 乘客端與儀表板共用：地圖、車輛圖層、WebSocket 自動重連
const COLOR = { a: '#16a34a', o: '#f59e0b', p: '#2563eb', t: '#9333ea' };
const STATUS_NAME = { a: '空車', o: '待接單', p: '前往載客', t: '載客中' };
const CENTER = [25.0478, 121.5400];

function makeMap() {
  const map = L.map('map', { preferCanvas: true, zoomControl: true }).setView(CENTER, 13);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18, attribution: '&copy; OpenStreetMap contributors',
  }).addTo(map);
  // 版面寬度改變時（例如縮放視窗）讓地圖重新計算尺寸
  new ResizeObserver(() => map.invalidateSize()).observe(document.getElementById('map'));
  return map;
}

// 車輛圖層：每收到一批位置就更新對應的圓點；超過 5 秒沒出現的車視為離線移除
class CarLayer {
  constructor(map) {
    this.map = map; this.cars = new Map(); this.focus = null;
    this.counts = { a: 0, o: 0, p: 0, t: 0 };
  }
  update(list) {
    const now = Date.now();
    const counts = { a: 0, o: 0, p: 0, t: 0 };
    for (const [id, lon, lat, s] of list) {
      counts[s] = (counts[s] || 0) + 1;
      let c = this.cars.get(id);
      if (!c) {
        c = { marker: L.circleMarker([lat, lon], { radius: 5, weight: 1, color: '#fff', fillOpacity: .95 }).addTo(this.map) };
        c.marker.bindTooltip('');
        this.cars.set(id, c);
      }
      c.marker.setLatLng([lat, lon]);
      const hot = id === this.focus;
      c.marker.setStyle({ fillColor: COLOR[s] || '#999', radius: hot ? 9 : 5, weight: hot ? 3 : 1, color: hot ? '#111827' : '#fff' });
      if (hot) c.marker.bringToFront();
      c.marker.setTooltipContent(`${id}｜${STATUS_NAME[s] || s}`);
      c.seen = now; c.lat = lat; c.lon = lon; c.s = s;
    }
    for (const [id, c] of this.cars) {
      if (now - c.seen > 5000) { c.marker.remove(); this.cars.delete(id); }
    }
    this.counts = counts;
  }
  get(id) { return this.cars.get(id); }
}

function connectWS(query, onMessage, onState) {
  let ws, closed = false;
  const open = () => {
    ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws${query}`);
    ws.onopen = () => onState && onState(true);
    ws.onmessage = (e) => onMessage(JSON.parse(e.data));
    ws.onclose = () => { onState && onState(false); if (!closed) setTimeout(open, 1500); };
  };
  open();
  return { close() { closed = true; ws.onclose = null; ws.close(); } };
}

function setConn(on) {
  const el = document.getElementById('conn');
  if (!el) return;
  el.classList.toggle('on', on);
  el.lastChild.textContent = on ? '即時連線中' : '連線中斷，重試中…';
}

const $ = (id) => document.getElementById(id);
const fmt = (n) => (n == null ? '–' : Number(n).toLocaleString('zh-TW'));
