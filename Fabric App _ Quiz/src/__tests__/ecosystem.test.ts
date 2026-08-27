import { describe, expect, it } from 'vitest';

import {
  ECOSYSTEM,
  NODE_INDEX,
  countDescendants,
  findNode,
  type EcoNode,
} from '@/data/ecosystem';
import { DOMAINS } from '@/data/exams';
import { FLASHCARDS } from '@/data/flashcards';

const cardIds = new Set(FLASHCARDS.map((c) => c.id));
const domainIds = new Set(DOMAINS.map((d) => d.id));

function walk(node: EcoNode, visit: (n: EcoNode, depth: number) => void, depth = 0) {
  visit(node, depth);
  node.children?.forEach((c) => walk(c, visit, depth + 1));
}

const all: EcoNode[] = [];
walk(ECOSYSTEM, (n) => all.push(n));

describe('ecosystem model', () => {
  it('has a unique id for every node', () => {
    const ids = all.map((n) => n.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('indexes every node with its full ancestry', () => {
    expect(NODE_INDEX.size).toBe(all.length);
    for (const node of all) {
      const entry = findNode(node.id)!;
      expect(entry.node).toBe(node);
      expect(entry.path[entry.path.length - 1]).toBe(node);
      expect(entry.path[0]).toBe(ECOSYSTEM);
    }
  });

  it('resolves a colour for every node, inheriting from its workload', () => {
    for (const node of all) {
      const entry = findNode(node.id)!;
      expect(entry.color).toMatch(/^#[0-9a-f]{6}$/i);
    }
    // A deep feature with no colour of its own takes the workload's.
    const feature = findNode('plat-throttling')!;
    const workload = findNode('platform')!;
    expect(feature.node.color).toBeUndefined();
    expect(feature.color).toBe(workload.color);
  });

  it('goes at least three levels deep, as the brief requires', () => {
    let maxDepth = 0;
    walk(ECOSYSTEM, (_, depth) => {
      maxDepth = Math.max(maxDepth, depth);
    });
    // root → workload → item → feature
    expect(maxDepth).toBeGreaterThanOrEqual(3);
  });

  it('drills the example path from the brief: Data Factory → pipelines → activities', () => {
    const factory = findNode('df')!;
    expect(factory.node.children?.map((c) => c.id)).toContain('df-pipeline');

    const pipeline = findNode('df-pipeline')!;
    expect(pipeline.node.children?.map((c) => c.id)).toEqual(
      expect.arrayContaining(['df-activities', 'df-connections'])
    );

    const activities = findNode('df-activities')!;
    expect(activities.path.map((n) => n.id)).toEqual([
      'fabric',
      'df',
      'df-pipeline',
      'df-activities',
    ]);
  });

  it('gives every node a summary and at least one fact', () => {
    for (const node of all) {
      expect(node.summary.length, `${node.id} summary`).toBeGreaterThan(30);
      expect(node.facts.length, `${node.id} facts`).toBeGreaterThan(0);
      for (const fact of node.facts) {
        expect(fact.length, `${node.id} fact`).toBeGreaterThan(10);
      }
    }
  });

  it('only references flashcards that exist', () => {
    for (const node of all) {
      for (const id of node.cards ?? []) {
        expect(cardIds.has(id), `${node.id} references missing card ${id}`).toBe(
          true
        );
      }
    }
  });

  it('only references domains that exist', () => {
    for (const node of all) {
      if (!node.domainId) continue;
      expect(domainIds.has(node.domainId), `${node.id} domain`).toBe(true);
    }
  });

  it('counts descendants correctly', () => {
    expect(countDescendants(findNode('df-activities')!.node)).toBe(0);
    const pipeline = findNode('df-pipeline')!.node;
    expect(countDescendants(pipeline)).toBe(pipeline.children!.length);
    expect(countDescendants(ECOSYSTEM)).toBe(all.length - 1);
  });

  it('keeps every ring small enough to stay readable', () => {
    for (const node of all) {
      const count = node.children?.length ?? 0;
      expect(count, `${node.id} has ${count} children`).toBeLessThanOrEqual(9);
    }
  });

  it('covers the eight top-level areas of the platform', () => {
    const workloads = ECOSYSTEM.children!.map((c) => c.id);
    expect(workloads).toEqual(
      expect.arrayContaining([
        'onelake',
        'df',
        'de',
        'dw',
        'rti',
        'pbi',
        'ds',
        'platform',
      ])
    );
  });
});
