// Orbit Pixels — a realtime collaborative INFINITE canvas. Pixels live at integer
// world cells; the client renders a camera-panned, full-screen window and only ever
// subscribes to `pixelsInView` for the chunks near the camera (re-subscribing as you
// pan). Presence (each user's cursor + the brush they're about to stamp) syncs through
// the `cursor` table. Everything rides Orbit.

import { useEffect, useMemo, useRef, useState, type PointerEvent as RPointerEvent } from 'react';
import { useQuery } from '@zeronsh/orbit/react';
import { getOrbit, type Pixel, type Cursor } from './orbit.ts';
import { CELL, CHUNK } from './shared.ts';

const PALETTE: `#${string}`[] = ['#171717', '#737373', '#cfcfcf', '#ef4444', '#f59e0b', '#facc15', '#22c55e', '#3b82f6', '#8b5cf6', '#ec4899'];
const SIZES = [1, 2, 3, 4];
const IDENTITY = ['#f43f5e', '#f59e0b', '#10b981', '#3b82f6', '#8b5cf6', '#ec4899', '#14b8a6', '#eab308'];

const STALE_MS = 5000;
const MOVE_MS = 45;
const HEARTBEAT_MS = 2000;

const hashId = (id: string) => {
  let h = 2166136261;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
};
const handleOf = (id: string) => `guest ${(hashId(id) % 9000) + 1000}`;
const identityOf = (id: string) => IDENTITY[hashId(id) % IDENTITY.length];

type Tool = 'paint' | 'erase' | 'pan';

export function Canvas({ me }: { me: { id: string } }) {
  const orbit = getOrbit(me.id);

  // The camera center, in world coords (cells). Lives in a ref for the rAF loop; the
  // center CHUNK is mirrored into state so the query re-subscribes when it changes.
  const camRef = useRef({ x: 0, y: 0 });
  const [center, setCenter] = useState({ cx: 0, cy: 0 });
  const centerRef = useRef(center);

  const pixelsQuery = useMemo(() => orbit.queries.pixelsInView({ cx: center.cx, cy: center.cy }), [orbit, center.cx, center.cy]);
  const cursorsQuery = useMemo(() => orbit.queries.cursorsInView({ cx: center.cx, cy: center.cy }), [orbit, center.cx, center.cy]);
  const pixels = useQuery(pixelsQuery) as unknown as Pixel[];
  const cursors = useQuery(cursorsQuery) as unknown as Cursor[];

  const [color, setColor] = useState(PALETTE[0]);
  const [size, setSize] = useState(1);
  const [tool, setTool] = useState<Tool>('paint');
  const [nowTick, setNow] = useState(0);

  const colorRef = useRef(color);
  colorRef.current = color;
  const sizeRef = useRef(size);
  sizeRef.current = size;
  const toolRef = useRef(tool);
  toolRef.current = tool;
  const cursorsRef = useRef(cursors);
  cursorsRef.current = cursors;

  const pixelMap = useMemo(() => {
    const m = new Map<string, { c: string; u: number }>();
    for (const p of pixels) m.set(p.id, { c: p.color, u: p.updated });
    return m;
  }, [pixels]);
  const pixelMapRef = useRef(pixelMap);
  pixelMapRef.current = pixelMap;

  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const mouseRef = useRef({ sx: -1, sy: -1, inside: false });
  const paintingRef = useRef(false);
  const panRef = useRef<{ active: boolean; sx: number; sy: number; cx: number; cy: number }>({ active: false, sx: 0, sy: 0, cx: 0, cy: 0 });
  const spaceRef = useRef(false);
  const strokeRef = useRef<Set<string>>(new Set());
  const lerpRef = useRef<Map<string, { x: number; y: number }>>(new Map());
  const lastSentRef = useRef(0);

  // --- presence ----------------------------------------------------------
  const worldAt = (clientX: number, clientY: number) => {
    const r = canvasRef.current!.getBoundingClientRect();
    const cam = camRef.current;
    return {
      wx: (clientX - r.left - r.width / 2) / CELL + cam.x,
      wy: (clientY - r.top - r.height / 2) / CELL + cam.y,
    };
  };
  const sendCursor = (wx: number, wy: number) =>
    orbit.mutate.moveCursor({
      x: wx,
      y: wy,
      color: colorRef.current,
      size: sizeRef.current,
      erasing: toolRef.current === 'erase' ? 1 : 0,
    });
  const clearCursor = () => orbit.mutate.clearCursor();

  const footprint = (cx: number, cy: number, s: number) => {
    const out: { x: number; y: number }[] = [];
    const x0 = cx - Math.floor((s - 1) / 2);
    const y0 = cy - Math.floor((s - 1) / 2);
    for (let dy = 0; dy < s; dy++) for (let dx = 0; dx < s; dx++) out.push({ x: x0 + dx, y: y0 + dy });
    return out;
  };

  const apply = (wx: number, wy: number) => {
    const cells = footprint(Math.floor(wx), Math.floor(wy), sizeRef.current);
    if (toolRef.current === 'erase') {
      const todo: { x: number; y: number }[] = [];
      for (const p of cells) {
        const id = `${p.x}:${p.y}`;
        if (strokeRef.current.has(id) || !pixelMapRef.current.has(id)) continue;
        strokeRef.current.add(id);
        pixelMapRef.current.delete(id);
        todo.push(p);
      }
      if (todo.length) orbit.mutate.erase({ cells: todo });
    } else {
      const c = colorRef.current;
      const todo: { x: number; y: number; color: `#${string}` }[] = [];
      for (const p of cells) {
        const id = `${p.x}:${p.y}`;
        if (strokeRef.current.has(id) || pixelMapRef.current.get(id)?.c === c) continue;
        strokeRef.current.add(id);
        pixelMapRef.current.set(id, { c, u: Date.now() });
        todo.push({ x: p.x, y: p.y, color: c });
      }
      if (todo.length) orbit.mutate.paint({ cells: todo });
    }
  };

  const updateCenter = () => {
    const cx = Math.floor(camRef.current.x / CHUNK);
    const cy = Math.floor(camRef.current.y / CHUNK);
    if (cx !== centerRef.current.cx || cy !== centerRef.current.cy) {
      centerRef.current = { cx, cy };
      setCenter({ cx, cy });
    }
  };

  // --- pointer handlers --------------------------------------------------
  const isPanGesture = (e: RPointerEvent) => toolRef.current === 'pan' || spaceRef.current || e.button === 1;

  const onDown = (e: RPointerEvent) => {
    (e.target as Element).setPointerCapture?.(e.pointerId);
    if (isPanGesture(e)) {
      panRef.current = { active: true, sx: e.clientX, sy: e.clientY, cx: camRef.current.x, cy: camRef.current.y };
      return;
    }
    paintingRef.current = true;
    strokeRef.current = new Set();
    const { wx, wy } = worldAt(e.clientX, e.clientY);
    mouseRef.current = { sx: e.clientX, sy: e.clientY, inside: true };
    apply(wx, wy);
    sendCursor(wx, wy);
  };
  const onMove = (e: RPointerEvent) => {
    mouseRef.current = { sx: e.clientX, sy: e.clientY, inside: true };
    if (panRef.current.active) {
      camRef.current = {
        x: panRef.current.cx - (e.clientX - panRef.current.sx) / CELL,
        y: panRef.current.cy - (e.clientY - panRef.current.sy) / CELL,
      };
      updateCenter();
    }
    const { wx, wy } = worldAt(e.clientX, e.clientY);
    if (paintingRef.current) apply(wx, wy);
    const t = performance.now();
    if (t - lastSentRef.current >= MOVE_MS) {
      lastSentRef.current = t;
      sendCursor(wx, wy);
    }
  };
  const onUp = () => {
    panRef.current.active = false;
    paintingRef.current = false;
    strokeRef.current = new Set();
  };
  const onLeave = () => {
    mouseRef.current.inside = false;
    paintingRef.current = false;
    panRef.current.active = false;
    clearCursor();
  };
  const onEnter = () => {
    mouseRef.current.inside = true;
  };

  // --- heartbeat + leave cleanup + spacebar pan --------------------------
  useEffect(() => {
    const hb = setInterval(() => {
      if (mouseRef.current.inside && document.visibilityState === 'visible') {
        const { wx, wy } = worldAt(mouseRef.current.sx, mouseRef.current.sy);
        sendCursor(wx, wy);
      }
    }, HEARTBEAT_MS);
    const tick = setInterval(() => setNow((n) => n + 1), 1000);
    const leave = () => clearCursor();
    const onVis = () => {
      if (document.visibilityState === 'hidden') clearCursor();
    };
    const kd = (e: KeyboardEvent) => {
      if (e.code === 'Space') spaceRef.current = true;
    };
    const ku = (e: KeyboardEvent) => {
      if (e.code === 'Space') spaceRef.current = false;
    };
    window.addEventListener('beforeunload', leave);
    window.addEventListener('pagehide', leave);
    window.addEventListener('keydown', kd);
    window.addEventListener('keyup', ku);
    document.addEventListener('visibilitychange', onVis);
    return () => {
      clearInterval(hb);
      clearInterval(tick);
      window.removeEventListener('beforeunload', leave);
      window.removeEventListener('pagehide', leave);
      window.removeEventListener('keydown', kd);
      window.removeEventListener('keyup', ku);
      document.removeEventListener('visibilitychange', onVis);
      clearCursor();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (mouseRef.current.inside) {
      const { wx, wy } = worldAt(mouseRef.current.sx, mouseRef.current.sy);
      sendCursor(wx, wy);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [color, size, tool]);

  // --- full-screen sizing + draw loop ------------------------------------
  useEffect(() => {
    const cv = canvasRef.current!;
    const ctx = cv.getContext('2d')!;
    let raf = 0;

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      cv.width = Math.round(window.innerWidth * dpr);
      cv.height = Math.round(window.innerHeight * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();
    window.addEventListener('resize', resize);

    // sprites: per-cell enter / exit / color-change animation, reconciled each frame.
    const sprites = new Map<string, { color: string; prev: string; born: number; changed: number; dying: number }>();
    const ENTER = 170;
    const EXIT = 150;
    const PULSE = 240;
    const easeOutBack = (t: number) => {
      const c1 = 1.70158;
      return 1 + (c1 + 1) * (t - 1) ** 3 + c1 * (t - 1) ** 2;
    };
    const hexRgb = (h: string) => {
      const n = parseInt(h.slice(1), 16);
      return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
    };
    const mix = (a: string, c: string, t: number) => {
      if (a === c || a[0] !== '#' || c[0] !== '#') return c;
      const [ar, ag, ab] = hexRgb(a);
      const [br, bg, bbl] = hexRgb(c);
      return `rgb(${Math.round(ar + (br - ar) * t)},${Math.round(ag + (bg - ag) * t)},${Math.round(ab + (bbl - ab) * t)})`;
    };

    const cell = (sx: number, sy: number, color: string, alpha: number, scale = 1) => {
      const inset = CELL * 0.06;
      const sz = (CELL - inset * 2) * scale;
      const off = (CELL - sz) / 2;
      ctx.globalAlpha = alpha;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.roundRect(sx + off, sy + off, sz, sz, CELL * 0.22 * scale);
      ctx.fill();
      ctx.globalAlpha = 1;
    };
    const ghostCell = (sx: number, sy: number, color: string, erasing: boolean) => {
      if (erasing) {
        const inset = CELL * 0.06;
        ctx.globalAlpha = 0.9;
        ctx.lineWidth = Math.max(1, CELL * 0.05);
        ctx.strokeStyle = '#9ca3af';
        ctx.setLineDash([CELL * 0.16, CELL * 0.16]);
        ctx.beginPath();
        ctx.roundRect(sx + inset, sy + inset, CELL - inset * 2, CELL - inset * 2, CELL * 0.22);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.globalAlpha = 1;
      } else {
        cell(sx, sy, color, 0.4);
      }
    };

    const draw = () => {
      const W = cv.clientWidth;
      const H = cv.clientHeight;
      const cam = camRef.current;
      ctx.clearRect(0, 0, W, H);
      const sxOf = (wx: number) => (wx - cam.x) * CELL + W / 2;
      const syOf = (wy: number) => (wy - cam.y) * CELL + H / 2;

      // faint dot at every cell corner, panning with the camera — spatial reference
      // on the otherwise-empty infinite canvas (dots, not lines).
      const x0 = Math.floor(cam.x - W / 2 / CELL) - 1;
      const x1 = Math.ceil(cam.x + W / 2 / CELL) + 1;
      const y0 = Math.floor(cam.y - H / 2 / CELL) - 1;
      const y1 = Math.ceil(cam.y + H / 2 / CELL) + 1;
      ctx.fillStyle = 'rgba(0,0,0,0.05)';
      for (let x = x0; x <= x1; x++) {
        const sx = sxOf(x);
        for (let y = y0; y <= y1; y++) ctx.fillRect(sx - 0.5, syOf(y) - 0.5, 1.5, 1.5);
      }

      const tnow = performance.now();
      const pm = pixelMapRef.current;

      // reconcile sprites
      for (const [id, v] of pm) {
        const sp = sprites.get(id);
        if (!sp) {
          // only animate a genuine new paint in; pixels that scroll into view (old
          // `updated`) just appear, so panning doesn't mass-pop.
          const fresh = Date.now() - v.u < 600;
          sprites.set(id, { color: v.c, prev: v.c, born: fresh ? tnow : tnow - ENTER, changed: 0, dying: 0 });
        } else if (sp.dying) {
          sp.dying = 0;
          sp.born = tnow - ENTER;
          sp.color = v.c;
          sp.prev = v.c;
          sp.changed = 0;
        } else if (sp.color !== v.c) {
          sp.prev = sp.color;
          sp.color = v.c;
          sp.changed = tnow;
        }
      }
      for (const [id, sp] of sprites) if (!pm.has(id) && !sp.dying) sp.dying = tnow;

      // painted pixels (culled)
      for (const [id, sp] of sprites) {
        const i = id.indexOf(':');
        const x = +id.slice(0, i);
        const y = +id.slice(i + 1);
        const sx = sxOf(x);
        const sy = syOf(y);
        if (sx <= -CELL || sx >= W || sy <= -CELL || sy >= H) {
          if (sp.dying && tnow - sp.dying >= EXIT) sprites.delete(id);
          continue;
        }
        let scale = 1;
        let alpha = 1;
        let col = sp.color;
        if (sp.dying) {
          const t = Math.min((tnow - sp.dying) / EXIT, 1);
          if (t >= 1) {
            sprites.delete(id);
            continue;
          }
          alpha = 1 - t;
          scale = 1 - 0.55 * t * t;
        } else {
          const e = Math.min((tnow - sp.born) / ENTER, 1);
          scale = e >= 1 ? 1 : Math.max(0, easeOutBack(e));
          alpha = e;
          if (sp.changed) {
            const p = Math.min((tnow - sp.changed) / PULSE, 1);
            scale *= 1 + 0.16 * Math.sin(p * Math.PI);
            col = mix(sp.prev, sp.color, p);
            if (p >= 1) {
              sp.changed = 0;
              sp.prev = sp.color;
            }
          }
        }
        cell(sx, sy, col, alpha, scale);
      }

      // remote cursors + previews (culled)
      const now = Date.now();
      for (const c of cursorsRef.current) {
        if (c.id === me.id || now - c.updated > STALE_MS) continue;
        let lp = lerpRef.current.get(c.id);
        if (!lp) {
          lp = { x: c.x, y: c.y };
          lerpRef.current.set(c.id, lp);
        }
        lp.x += (c.x - lp.x) * 0.3;
        lp.y += (c.y - lp.y) * 0.3;
        const px = sxOf(lp.x);
        const py = syOf(lp.y);
        if (px < -CELL * 6 || px > W + CELL * 6 || py < -CELL * 6 || py > H + CELL * 6) continue;
        const fx = Math.floor(lp.x);
        const fy = Math.floor(lp.y);
        for (const p of footprint(fx, fy, c.size)) ghostCell(sxOf(p.x), syOf(p.y), c.color, c.erasing === 1);
        pointer(ctx, px, py, identityOf(c.id), handleOf(c.id));
      }

      // my own brush preview
      const m = mouseRef.current;
      if (m.inside && toolRef.current !== 'pan') {
        const wx = (m.sx - cv.getBoundingClientRect().left - W / 2) / CELL + cam.x;
        const wy = (m.sy - cv.getBoundingClientRect().top - H / 2) / CELL + cam.y;
        const fx = Math.floor(wx);
        const fy = Math.floor(wy);
        for (const p of footprint(fx, fy, sizeRef.current)) ghostCell(sxOf(p.x), syOf(p.y), colorRef.current, toolRef.current === 'erase');
      }

      raf = requestAnimationFrame(draw);
    };
    raf = requestAnimationFrame(draw);
    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener('resize', resize);
    };
  }, [me.id]);

  const live = useMemo(() => {
    const t = Date.now();
    const ids = new Set<string>([me.id]);
    for (const c of cursors) if (t - c.updated < STALE_MS) ids.add(c.id);
    return ids.size;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cursors, me.id, nowTick]);

  const cursorClass = tool === 'pan' ? 'pan' : tool === 'erase' ? 'erase' : 'paint';

  return (
    <div className="app">
      <canvas
        ref={canvasRef}
        className={`canvas ${cursorClass}`}
        onPointerMove={onMove}
        onPointerDown={onDown}
        onPointerUp={onUp}
        onPointerLeave={onLeave}
        onPointerEnter={onEnter}
        onContextMenu={(e) => e.preventDefault()}
      />

      <header className="bar">
        <div className="bar-right">
          <span className="live">
            <span className="live-dot" />
            {live} live
          </span>
          <span className="me">
            <span className="me-dot" style={{ background: identityOf(me.id) }} />
            {handleOf(me.id)}
          </span>
        </div>
      </header>

      <div className="toolbar">
        <div className="seg">
          <button className={`tool ${tool === 'paint' ? 'on' : ''}`} onClick={() => setTool('paint')} title="Brush">
            <BrushIcon />
          </button>
          <button className={`tool ${tool === 'erase' ? 'on' : ''}`} onClick={() => setTool('erase')} title="Eraser">
            <EraserIcon />
          </button>
          <button className={`tool ${tool === 'pan' ? 'on' : ''}`} onClick={() => setTool('pan')} title="Pan (or hold Space)">
            <HandIcon />
          </button>
        </div>
        <span className="div" />
        <div className={`swatches ${tool === 'erase' || tool === 'pan' ? 'dim' : ''}`}>
          {PALETTE.map((c) => (
            <button
              key={c}
              className={`swatch ${color === c && tool === 'paint' ? 'on' : ''}`}
              style={{ background: c }}
              onClick={() => {
                setColor(c);
                setTool('paint');
              }}
              title={c}
            />
          ))}
        </div>
        <span className="div" />
        <div className="sizes">
          {SIZES.map((s) => (
            <button key={s} className={`size ${size === s ? 'on' : ''}`} onClick={() => setSize(s)} title={`${s}×${s}`}>
              <span className="size-dot" style={{ width: 4 + s * 3, height: 4 + s * 3 }} />
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function pointer(ctx: CanvasRenderingContext2D, px: number, py: number, color: string, label: string) {
  ctx.save();
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.arc(px, py, Math.max(3, CELL * 0.15), 0, Math.PI * 2);
  ctx.fill();
  ctx.lineWidth = Math.max(1, CELL * 0.04);
  ctx.strokeStyle = '#fff';
  ctx.stroke();
  const fs = Math.max(10, CELL * 0.42);
  ctx.font = `500 ${fs}px 'Geist', ui-sans-serif, system-ui, sans-serif`;
  const padX = fs * 0.55;
  const h = fs * 1.7;
  const w = ctx.measureText(label).width + padX * 2;
  const lx = px + CELL * 0.28;
  const ly = py + CELL * 0.28;
  ctx.globalAlpha = 0.96;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.roundRect(lx, ly, w, h, h * 0.34);
  ctx.fill();
  ctx.globalAlpha = 1;
  ctx.fillStyle = '#fff';
  ctx.textBaseline = 'middle';
  ctx.fillText(label, lx + padX, ly + h / 2 + fs * 0.04);
  ctx.restore();
}

function BrushIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9.5 14.5 3 21s3.5.5 5.5-1.5S10 14 10 14" />
      <path d="m14 11 6.3-6.3a1.7 1.7 0 0 0-2.4-2.4L11.6 8.6" />
      <path d="m9 13 2 2" />
    </svg>
  );
}
function EraserIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 16.5 13 7.5l4 4-7.5 7.5H6z" />
      <path d="m13 7.5 3-3a1.6 1.6 0 0 1 2.3 0l1.7 1.7a1.6 1.6 0 0 1 0 2.3l-3 3" />
      <path d="M7 20h12" />
    </svg>
  );
}
function HandIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M18 11V6.5a1.5 1.5 0 0 0-3 0V11m0-.5V5a1.5 1.5 0 0 0-3 0v6m0-.5V5.5a1.5 1.5 0 0 0-3 0V12" />
      <path d="M9 11V8a1.5 1.5 0 0 0-3 0v8a6 6 0 0 0 6 6h1a6 6 0 0 0 6-6v-3" />
    </svg>
  );
}
