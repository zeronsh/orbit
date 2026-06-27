// Drizzle schema for the tables Orbit replicates (see ../postgres/01-init.sql).
// This is the SOURCE OF TRUTH for the Orbit client schema: run
// `pnpm generate:schema` to regenerate ../src/schema.gen.ts from it.
//
// Orbit field names are the database column names (Orbit syncs raw Postgres
// rows), so they match the columns declared here and in 01-init.sql.

import { bigint, doublePrecision, integer, pgTable, text } from 'drizzle-orm/pg-core';

// One painted cell. `id` is "x:y" so a place is an upsert and an erase is a delete.
// `.$type<>()` here is inherited into the generated Orbit schema (codegen only):
// `color` becomes `string<`#${string}`>()`, so the client types colors as hex.
export const pixel = pgTable('pixel', {
  id: text('id').primaryKey(),
  x: integer('x').notNull(),
  y: integer('y').notNull(),
  color: text('color').$type<`#${string}`>().notNull(),
  updated: bigint('updated', { mode: 'number' }).notNull(),
});

// Ephemeral presence: one row per connected user, keyed by user id. x/y are world
// coords; a heartbeat keeps `updated` fresh.
export const cursor = pgTable('cursor', {
  id: text('id').primaryKey(),
  x: doublePrecision('x').notNull(),
  y: doublePrecision('y').notNull(),
  color: text('color').$type<`#${string}`>().notNull(),
  size: integer('size').notNull(),
  // 0 = drawing, 1 = erasing — inherited as `number<0 | 1>()`.
  erasing: integer('erasing').$type<0 | 1>().notNull().default(0),
  updated: bigint('updated', { mode: 'number' }).notNull(),
});
