import { useEffect, useRef } from 'react';

import { findNode } from '@/data/ecosystem';

import { EcosystemScene } from './EcosystemScene';

export interface EcosystemCanvasProps {
  /** The node whose children are currently laid out in the ring. */
  centerId: string;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDrill: (id: string) => void;
  /** Bump to snap the camera back to its default framing. */
  resetNonce: number;
}

/**
 * Mounts the three.js scene and keeps it in step with React state.
 *
 * Default-exported so it can be React.lazy'd — three.js is by far the largest
 * dependency in the app and has no business in the initial bundle.
 */
export default function EcosystemCanvas({
  centerId,
  selectedId,
  onSelect,
  onDrill,
  resetNonce,
}: EcosystemCanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sceneRef = useRef<EcosystemScene | null>(null);

  // Handlers change identity on every render; the scene is built once, so it
  // reads them through a ref rather than being rebuilt.
  const handlersRef = useRef({ onSelect, onDrill });
  useEffect(() => {
    handlersRef.current = { onSelect, onDrill };
  }, [onSelect, onDrill]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const scene = new EcosystemScene(container, {
      onSelect: (id) => handlersRef.current.onSelect(id),
      onDrill: (id) => handlersRef.current.onDrill(id),
      onHover: () => {},
    });
    sceneRef.current = scene;

    return () => {
      sceneRef.current = null;
      scene.dispose();
    };
  }, []);

  useEffect(() => {
    const scene = sceneRef.current;
    const entry = findNode(centerId);
    if (!scene || !entry) return;

    scene.nudgeIn();
    scene.setLevel({
      center: entry.node,
      centerColor: entry.color,
      children: (entry.node.children ?? []).map((child) => ({
        node: child,
        color: findNode(child.id)?.color ?? entry.color,
      })),
      originId: centerId,
    });
  }, [centerId]);

  useEffect(() => {
    sceneRef.current?.setSelected(selectedId);
  }, [selectedId]);

  useEffect(() => {
    if (resetNonce > 0) sceneRef.current?.resetView();
  }, [resetNonce]);

  return (
    <div
      ref={containerRef}
      className="relative h-full w-full"
      role="application"
      aria-label="Interactive 3D model of the Microsoft Fabric ecosystem"
    />
  );
}
