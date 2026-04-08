This is a [Next.js](https://nextjs.org) app for MaxCashBack.

## Getting Started

Create `web/.env.local` with your Supabase and optional AdSense settings:

```bash
cp .env.local.example .env.local
```

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## AdSense

Ads are disabled unless all of these are configured:

- `NEXT_PUBLIC_ENABLE_ADS=true`
- `NEXT_PUBLIC_ADSENSE_CLIENT`
- the slot ID for the placement you want to render

Current placements:

- homepage top
- homepage inline
- store page inline

The app loads the AdSense script once from the root layout and each placement is env-gated, so local development still works with ads turned off.

## AdSense Rollout Notes

This integration is implemented but intentionally not live until the production domain is ready.

Important constraints:

- AdSense approval and real ad serving require a deployed production domain
- localhost is only for checking that the script path and ad containers render safely
- we chose manual display ad units instead of Auto ads because the app already has specific placements

When ready to launch ads:

1. Add the production domain in AdSense
2. Complete site verification and request review
3. Create these responsive display ad units in AdSense:
   - homepage top
   - homepage inline
   - store inline
4. Set the matching env vars in production
5. Enable `NEXT_PUBLIC_ENABLE_ADS=true`

Recommended production follow-up:

- add `ads.txt`
- verify there are no layout shifts on mobile
- confirm only one AdSense script is loaded and each slot initializes once
