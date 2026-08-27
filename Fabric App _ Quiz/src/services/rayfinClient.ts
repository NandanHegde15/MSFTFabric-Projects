import { RayfinClient } from '@microsoft/rayfin-client';

import type { FabricQuizSchema } from '../../rayfin/data/schema';

export interface RayfinClientConfig {
  baseUrl: string;
  publishableKey: string;
  /** True when the API URL points at localhost. Exposed via {@link isLocalBackend}. */
  localDev: boolean;
}

let client: RayfinClient<FabricQuizSchema> | null = null;
let localDev = false;

export function initRayfinClient(
  config: RayfinClientConfig
): RayfinClient<FabricQuizSchema> {
  if (client) {
    throw new Error('Rayfin client is already initialized.');
  }
  client = new RayfinClient<FabricQuizSchema>({
    baseUrl: config.baseUrl,
    publishableKey: config.publishableKey,
    authStorage: true,
  });
  localDev = config.localDev;
  return client;
}

export function getRayfinClient(): RayfinClient<FabricQuizSchema> {
  if (!client) {
    throw new Error(
      'Rayfin client not initialized. Call bootstrapAuth() first.'
    );
  }
  return client;
}

/** True when the app was bootstrapped against a localhost backend. */
export function isLocalBackend(): boolean {
  return localDev;
}
