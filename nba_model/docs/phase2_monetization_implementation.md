# Phase 2 Implementation Notes

Date: 2026-03-11

## Delivered
- Account auth endpoints with password hashing and secure session cookie.
- Subscription model with `free` / `pro` plans.
- Usage metering by event key:
  - `assistant_messages`
  - `prediction_runs`
  - `benchmark_runs`
  - `daily_refresh_runs`
  - `live_sync_actions`
- Optional paywall enforcement through `PAYWALL_ENFORCEMENT`.
- Stripe checkout and billing portal session creation.
- Stripe webhook verification + subscription lifecycle persistence.
- UI account card for register/login/logout/upgrade/portal and live usage visibility.

## Data Storage
- SQLite database:
  - `data/commerce.db`
- Core tables:
  - `users`
  - `sessions`
  - `subscriptions`
  - `usage_events`
  - `stripe_events`

## Stripe Notes
- Checkout endpoint returns a Stripe-hosted URL.
- Webhook endpoint requires valid Stripe signature:
  - Header: `Stripe-Signature`
  - Secret: `STRIPE_WEBHOOK_SECRET`
- Subscription rows are updated from:
  - `checkout.session.completed`
  - `customer.subscription.created`
  - `customer.subscription.updated`
  - `customer.subscription.deleted`
  - `invoice.paid` (limited linkage)

## Rollout Strategy
1. Keep `PAYWALL_ENFORCEMENT=0` while testing in local/staging.
2. Verify checkout + webhook updates in Stripe test mode.
3. Switch to `PAYWALL_ENFORCEMENT=1` after validating plan transitions and usage limits.
