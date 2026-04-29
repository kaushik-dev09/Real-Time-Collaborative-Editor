import { createClient } from 'redis';

export const redisPub = createClient({ url: process.env.REDIS_URL });
export const redisSub = redisPub.duplicate();

export async function connectRedis() {
  await Promise.all([redisPub.connect(), redisSub.connect()]);
  console.log('[Redis] Connected pub/sub clients');
}
