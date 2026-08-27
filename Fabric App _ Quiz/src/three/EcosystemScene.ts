import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';

import type { EcoNode, NodeShape } from '@/data/ecosystem';

export interface LevelInput {
  /** The node at the centre of this level. */
  center: EcoNode;
  centerColor: string;
  children: { node: EcoNode; color: string }[];
  /** Where the children should appear to emerge from, in world space. */
  originId?: string;
}

interface NodeVisual {
  id: string;
  node: EcoNode;
  group: THREE.Group;
  mesh: THREE.Mesh;
  material: THREE.MeshStandardMaterial;
  label: HTMLButtonElement;
  home: THREE.Vector3;
  from: THREE.Vector3;
  /** 0 → 1 spawn progress. */
  t: number;
  bob: number;
  baseScale: number;
  hovered: boolean;
  selected: boolean;
  isCenter: boolean;
}

const easeOut = (t: number) => 1 - Math.pow(1 - t, 3);

function geometryFor(shape: NodeShape | undefined, radius: number): THREE.BufferGeometry {
  switch (shape) {
    case 'sphere':
      return new THREE.SphereGeometry(radius, 40, 28);
    case 'cylinder':
      return new THREE.CylinderGeometry(radius * 0.82, radius * 0.82, radius * 1.5, 34);
    case 'octahedron':
      return new THREE.OctahedronGeometry(radius * 1.16, 0);
    case 'torus':
      return new THREE.TorusGeometry(radius * 0.82, radius * 0.33, 18, 44);
    case 'box':
    default:
      return new THREE.BoxGeometry(radius * 1.5, radius * 1.5, radius * 1.5);
  }
}

function radiusFor(node: EcoNode, isCenter: boolean): number {
  if (isCenter) return node.kind === 'root' ? 1.5 : 1.25;
  if (node.kind === 'workload') return 0.82;
  if (node.kind === 'item') return 0.66;
  return 0.56;
}

/**
 * The Fabric ecosystem rendered as one ring of children orbiting the node you
 * are currently inside. Drilling in re-seeds the ring from the selected node's
 * position, which reads as the parent coming apart into its components.
 *
 * Deliberately imperative: labels are DOM elements repositioned every frame, so
 * React never re-renders during animation.
 */
export class EcosystemScene {
  private renderer: THREE.WebGLRenderer;
  private scene = new THREE.Scene();
  private camera: THREE.PerspectiveCamera;
  private controls: OrbitControls;
  private raycaster = new THREE.Raycaster();
  private pointer = new THREE.Vector2(-2, -2);
  // Timer, not Clock: Clock is deprecated in three and warns on every load.
  private timer = new THREE.Timer();

  private nodes: NodeVisual[] = [];
  private links: THREE.Line[] = [];
  private outgoing: THREE.Group[] = [];
  private labelLayer: HTMLDivElement;
  private frame = 0;
  private disposed = false;
  private hoveredId: string | null = null;

  constructor(
    private container: HTMLDivElement,
    private handlers: {
      onSelect: (id: string) => void;
      onDrill: (id: string) => void;
      onHover: (id: string | null) => void;
    }
  ) {
    const { clientWidth: w, clientHeight: h } = container;

    this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.setSize(w, h);
    this.renderer.domElement.style.display = 'block';
    this.renderer.domElement.style.touchAction = 'none';
    container.appendChild(this.renderer.domElement);

    this.labelLayer = document.createElement('div');
    Object.assign(this.labelLayer.style, {
      position: 'absolute',
      inset: '0',
      pointerEvents: 'none',
      overflow: 'hidden',
    });
    container.appendChild(this.labelLayer);

    this.camera = new THREE.PerspectiveCamera(45, w / h, 0.1, 260);
    this.camera.position.set(0, 8.5, 15.5);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.08;
    this.controls.minDistance = 7;
    this.controls.maxDistance = 34;
    this.controls.maxPolarAngle = 1.45;
    this.controls.minPolarAngle = 0.15;
    this.controls.enablePan = false;
    this.controls.target.set(0, 0.4, 0);

    this.scene.add(new THREE.HemisphereLight(0xffffff, 0x94a3b8, 1.5));
    const key = new THREE.DirectionalLight(0xffffff, 2.1);
    key.position.set(6, 12, 8);
    this.scene.add(key);
    const rim = new THREE.DirectionalLight(0xc7d2fe, 1.1);
    rim.position.set(-8, 4, -6);
    this.scene.add(rim);

    this.addFloor();

    this.renderer.domElement.addEventListener('pointermove', this.onPointerMove);
    this.renderer.domElement.addEventListener('pointerleave', this.onPointerLeave);
    this.renderer.domElement.addEventListener('click', this.onClick);
    this.renderer.domElement.addEventListener('dblclick', this.onDoubleClick);

    this.resizeObserver.observe(container);
    this.loop();
  }

  private addFloor() {
    const grid = new THREE.PolarGridHelper(15, 8, 6, 64, 0xcbd5e1, 0xe2e8f0);
    const material = grid.material as THREE.Material | THREE.Material[];
    const applyFade = (m: THREE.Material) => {
      m.transparent = true;
      m.opacity = 0.5;
      m.depthWrite = false;
    };
    if (Array.isArray(material)) material.forEach(applyFade);
    else applyFade(material);
    grid.position.y = -1.6;
    this.scene.add(grid);
  }

  private lastSize = new THREE.Vector2();

  /**
   * Match the drawing buffer to the container.
   *
   * Called both from the ResizeObserver and from every frame. The observer
   * gives an immediate response; the per-frame check is the guarantee, because
   * observer deliveries are suspended while a tab or pane is hidden and a
   * resize that happens in that window would otherwise never be corrected.
   */
  private syncSize() {
    const { clientWidth: w, clientHeight: h } = this.container;
    if (w === 0 || h === 0) return;

    this.renderer.getSize(this.lastSize);
    if (Math.abs(this.lastSize.x - w) < 1 && Math.abs(this.lastSize.y - h) < 1) {
      return;
    }

    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(w, h);
  }

  private resizeObserver = new ResizeObserver(() => this.syncSize());

  // -------------------------------------------------------------- building

  setLevel(input: LevelInput) {
    const origin =
      (input.originId && this.nodes.find((n) => n.id === input.originId)?.group.position.clone()) ||
      new THREE.Vector3(0, 0, 0);

    this.retireCurrentLevel();
    this.hoveredId = null;

    const count = input.children.length;
    const ringRadius = Math.max(4.6, 2.1 + count * 0.62);

    this.addNode(input.center, input.centerColor, new THREE.Vector3(0, 0.4, 0), origin, true);

    input.children.forEach((child, i) => {
      const angle = (i / Math.max(1, count)) * Math.PI * 2 - Math.PI / 2;
      const home = new THREE.Vector3(
        Math.cos(angle) * ringRadius,
        0.4 + Math.sin(i * 1.7) * 0.35,
        Math.sin(angle) * ringRadius
      );
      this.addNode(child.node, child.color, home, origin, false);
    });

    this.buildLinks();
    this.controls.maxDistance = Math.max(28, ringRadius * 3.2);
    this.controls.minDistance = Math.max(6, ringRadius * 0.55);
  }

  private addNode(
    node: EcoNode,
    color: string,
    home: THREE.Vector3,
    from: THREE.Vector3,
    isCenter: boolean
  ) {
    const radius = radiusFor(node, isCenter);
    const geometry = geometryFor(node.shape, radius);
    const material = new THREE.MeshStandardMaterial({
      color: new THREE.Color(color),
      roughness: 0.34,
      metalness: 0.18,
      emissive: new THREE.Color(color),
      emissiveIntensity: 0.06,
      transparent: true,
      opacity: 1,
    });

    const mesh = new THREE.Mesh(geometry, material);
    mesh.rotation.y = Math.random() * Math.PI;

    const group = new THREE.Group();
    group.add(mesh);
    group.position.copy(from);
    group.scale.setScalar(0.01);
    this.scene.add(group);

    const label = document.createElement('button');
    label.type = 'button';
    label.textContent = node.name;
    label.dataset.nodeId = node.id;
    Object.assign(label.style, {
      position: 'absolute',
      transform: 'translate(-50%, -50%)',
      pointerEvents: 'auto',
      cursor: 'pointer',
      whiteSpace: 'nowrap',
      border: '1px solid rgba(148,163,184,0.45)',
      borderRadius: '999px',
      background: 'rgba(255,255,255,0.92)',
      color: '#0f172a',
      font: '500 12px/1.2 Inter, system-ui, sans-serif',
      padding: '3px 9px',
      boxShadow: '0 1px 3px rgba(15,23,42,0.12)',
      opacity: '0',
      transition: 'background-color .15s, color .15s, border-color .15s',
    });
    label.addEventListener('click', (e) => {
      e.stopPropagation();
      this.handlers.onSelect(node.id);
    });
    label.addEventListener('dblclick', (e) => {
      e.stopPropagation();
      this.handlers.onDrill(node.id);
    });
    this.labelLayer.appendChild(label);

    this.nodes.push({
      id: node.id,
      node,
      group,
      mesh,
      material,
      label,
      home: home.clone(),
      from: from.clone(),
      t: 0,
      bob: Math.random() * Math.PI * 2,
      baseScale: 1,
      hovered: false,
      selected: false,
      isCenter,
    });
  }

  private buildLinks() {
    const centre = this.nodes.find((n) => n.isCenter);
    if (!centre) return;

    for (const node of this.nodes) {
      if (node.isCenter) continue;
      const geometry = new THREE.BufferGeometry().setFromPoints([
        centre.home.clone(),
        node.home.clone(),
      ]);
      const line = new THREE.Line(
        geometry,
        new THREE.LineBasicMaterial({
          color: new THREE.Color(node.material.color),
          transparent: true,
          opacity: 0,
        })
      );
      this.scene.add(line);
      this.links.push(line);
    }
  }

  /** Fade the current level out and let it clean itself up. */
  private retireCurrentLevel() {
    for (const node of this.nodes) {
      node.label.remove();
      const group = node.group;
      this.outgoing.push(group);
      // Mark the start of the fade; the loop finishes and disposes it.
      group.userData.retireAt = this.timer.getElapsed();
    }
    this.nodes = [];

    for (const link of this.links) {
      this.scene.remove(link);
      link.geometry.dispose();
      (link.material as THREE.Material).dispose();
    }
    this.links = [];
  }

  // ---------------------------------------------------------------- events

  private onPointerMove = (event: PointerEvent) => {
    const rect = this.renderer.domElement.getBoundingClientRect();
    this.pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    this.pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
  };

  private onPointerLeave = () => {
    this.pointer.set(-2, -2);
  };

  private pick(): NodeVisual | null {
    this.raycaster.setFromCamera(this.pointer, this.camera);
    const hits = this.raycaster.intersectObjects(
      this.nodes.map((n) => n.mesh),
      false
    );
    if (hits.length === 0) return null;
    const mesh = hits[0].object;
    return this.nodes.find((n) => n.mesh === mesh) ?? null;
  }

  private onClick = () => {
    const hit = this.pick();
    if (hit) this.handlers.onSelect(hit.id);
  };

  private onDoubleClick = () => {
    const hit = this.pick();
    if (hit) this.handlers.onDrill(hit.id);
  };

  setSelected(id: string | null) {
    for (const node of this.nodes) node.selected = node.id === id;
  }

  // ------------------------------------------------------------------ loop

  private loop = () => {
    if (this.disposed) return;
    this.frame = requestAnimationFrame(this.loop);
    this.syncSize();

    this.timer.update();
    const dt = Math.min(this.timer.getDelta(), 0.05);
    const time = this.timer.getElapsed();

    const hit = this.pick();
    const hoveredId = hit?.id ?? null;
    if (hoveredId !== this.hoveredId) {
      this.hoveredId = hoveredId;
      this.renderer.domElement.style.cursor = hoveredId ? 'pointer' : 'grab';
      this.handlers.onHover(hoveredId);
    }

    for (const node of this.nodes) {
      node.hovered = node.id === this.hoveredId;
      node.t = Math.min(1, node.t + dt * 1.7);
      const e = easeOut(node.t);

      node.group.position.lerpVectors(node.from, node.home, e);
      node.group.position.y =
        node.home.y * e + Math.sin(time * 1.1 + node.bob) * 0.075 * e;

      const emphasis = node.hovered ? 1.16 : node.selected ? 1.09 : 1;
      const target = node.baseScale * emphasis * e;
      node.group.scale.lerp(new THREE.Vector3(target, target, target), 0.25);

      node.mesh.rotation.y += dt * (node.isCenter ? 0.16 : 0.06);

      const glow = node.hovered ? 0.42 : node.selected ? 0.3 : 0.06;
      node.material.emissiveIntensity +=
        (glow - node.material.emissiveIntensity) * 0.18;
      node.material.opacity = e;

      this.positionLabel(node, e);
    }

    const linkOpacity = this.nodes.length > 0 ? Math.min(1, this.nodes[0].t) * 0.28 : 0;
    for (const link of this.links) {
      (link.material as THREE.LineBasicMaterial).opacity = linkOpacity;
    }

    this.tickOutgoing(time);
    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  };

  private positionLabel(node: NodeVisual, spawn: number) {
    const world = node.group.position.clone();
    world.y += node.isCenter ? 1.85 : 1.0;
    const projected = world.project(this.camera);

    const behind = projected.z > 1;
    if (behind) {
      node.label.style.opacity = '0';
      node.label.style.pointerEvents = 'none';
      return;
    }

    const rect = this.renderer.domElement;
    const x = (projected.x * 0.5 + 0.5) * rect.clientWidth;
    const y = (-projected.y * 0.5 + 0.5) * rect.clientHeight;

    node.label.style.left = `${x}px`;
    node.label.style.top = `${y}px`;
    node.label.style.opacity = String(spawn);
    node.label.style.pointerEvents = spawn > 0.9 ? 'auto' : 'none';

    const active = node.hovered || node.selected;
    node.label.style.background = active
      ? `#${node.material.color.getHexString()}`
      : 'rgba(255,255,255,0.92)';
    node.label.style.color = active ? '#ffffff' : '#0f172a';
    node.label.style.borderColor = active
      ? 'transparent'
      : 'rgba(148,163,184,0.45)';
    node.label.style.fontWeight = node.isCenter ? '600' : '500';
  }

  /** Shrink and dispose the previous level. */
  private tickOutgoing(time: number) {
    for (let i = this.outgoing.length - 1; i >= 0; i--) {
      const group = this.outgoing[i];
      const age = time - (group.userData.retireAt as number);
      const k = Math.max(0, 1 - age / 0.4);

      group.scale.setScalar(Math.max(0.001, group.scale.x * 0.86));
      group.traverse((child) => {
        const mesh = child as THREE.Mesh;
        const material = mesh.material as THREE.MeshStandardMaterial | undefined;
        if (material && 'opacity' in material) material.opacity = k;
      });

      if (k <= 0) {
        this.scene.remove(group);
        group.traverse((child) => {
          const mesh = child as THREE.Mesh;
          mesh.geometry?.dispose();
          const material = mesh.material as THREE.Material | undefined;
          material?.dispose();
        });
        this.outgoing.splice(i, 1);
      }
    }
  }

  /** Pull the camera in a little, to sell the zoom before a level change. */
  nudgeIn() {
    const direction = this.camera.position.clone().sub(this.controls.target);
    const distance = Math.max(this.controls.minDistance, direction.length() * 0.82);
    this.camera.position.copy(
      this.controls.target.clone().add(direction.setLength(distance))
    );
  }

  resetView() {
    this.camera.position.set(0, 8.5, 15.5);
    this.controls.target.set(0, 0.4, 0);
    this.controls.update();
  }

  dispose() {
    this.disposed = true;
    cancelAnimationFrame(this.frame);
    this.resizeObserver.disconnect();

    this.renderer.domElement.removeEventListener('pointermove', this.onPointerMove);
    this.renderer.domElement.removeEventListener('pointerleave', this.onPointerLeave);
    this.renderer.domElement.removeEventListener('click', this.onClick);
    this.renderer.domElement.removeEventListener('dblclick', this.onDoubleClick);

    this.retireCurrentLevel();
    this.scene.traverse((child) => {
      const mesh = child as THREE.Mesh;
      mesh.geometry?.dispose();
      const material = mesh.material as THREE.Material | THREE.Material[] | undefined;
      if (Array.isArray(material)) material.forEach((m) => m.dispose());
      else material?.dispose();
    });

    this.controls.dispose();
    this.renderer.dispose();
    this.labelLayer.remove();
    this.renderer.domElement.remove();
  }
}
