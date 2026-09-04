'use strict'
/**
 * Who owns a pin, and what they are allowed — the one seam where this bridge
 * meets whatever service is embedding it.
 *
 * The bridge itself has no idea what a customer is. Everything it needs is
 * two values it is handed at construction:
 *
 *   plans      { [planId]: { title, publicPins, storageBytes, publicBytesPerMonth } }
 *   accountOf  (ownerId) => { plan: planId } | null
 *
 *   publicPins           may this owner have public pins at all (402 if not)
 *   storageBytes         ceiling on the CAR bytes this owner is responsible
 *                        for; enforced BEFORE the bytes are bought, by
 *                        cutting the upload stream off at the room left
 *   publicBytesPerMonth  egress allowance; past it the edge slows, past
 *                        1.5x it answers 429. 0 means uncapped.
 *                        It NEVER unpins: data is not at risk from a
 *                        bandwidth event.
 *
 * `single()` is the default for someone running this for themselves: one
 * owner, no ceilings. Swap it for your own billing and nothing else changes.
 */

const UNCAPPED = {
  title: 'default',
  publicPins: true,
  storageBytes: Number.MAX_SAFE_INTEGER,
  publicBytesPerMonth: 0
}

/** Everything belongs to one owner with no limits. */
function single (ownerId = 'default') {
  return {
    ownerId,
    plans: { default: UNCAPPED },
    accountOf: (id) => (id === ownerId ? { id, plan: 'default' } : null)
  }
}

/**
 * A fixed table of plans, and a lookup you provide.
 * @param {object} plans        planId -> plan
 * @param {(id: string) => object|null} accountOf
 */
function of (plans, accountOf) {
  for (const [id, p] of Object.entries(plans)) {
    for (const k of ['title', 'publicPins', 'storageBytes', 'publicBytesPerMonth']) {
      if (!(k in p)) throw new Error(`plan ${id}: no ${k}`)
    }
  }
  return { plans, accountOf }
}

module.exports = { single, of, UNCAPPED }
