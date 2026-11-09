# Deploying LinkPulse for free

| Piece | Service | Free tier notes |
|---|---|---|
| API, dashboard, analytics consumer | [Render](https://render.com) web service | Sleeps after ~15 min idle; first request then takes ~1 min |
| PostgreSQL | [Neon](https://neon.tech) | Scales to zero; wakes in about a second |
| Redis | [Upstash](https://upstash.com) | Request quota per month; plenty for a demo |
| Kafka | [Aiven](https://aiven.io/free-kafka) | Small throughput limits; powers off when idle |

None of these need a credit card. The whole setup takes about 20 minutes. Do the steps in order and keep a text file open to collect the values.

> **Never commit these values to git.** They go only into Render's environment settings.

## 1. Postgres on Neon → `DATABASE_URL` ✅ done

Project `linkpulse` (AWS Singapore) is linked to this repo with `neon init`. The app was tested end to end against it: tables are created, links redirect, and analytics roll up.

To get the value for Render, click **Connect** in the Neon console with **Connection pooling on**, and copy the string (host contains `-pooler`). Pooled is Neon's recommendation for app traffic, and it was verified with this app's `pgx` driver. Locally, `neon env pull` writes the same values to `.env.local` (git-ignored).

## 2. Redis on Upstash → `REDIS_URL`

Upstash's free plan allows one Redis database. If you already have one (for another project), **reuse it**: LinkPulse prefixes every key with `lp:` (`REDIS_NAMESPACE`), so it never touches the other app's keys. The two apps share the free monthly command quota.

On the database page, copy the `rediss://` URL (two s's means TLS):
`rediss://default:<password>@<name>.upstash.io:6379`

## 3. Kafka on Aiven → `KAFKA_BROKERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_CA_CERT`

1. Sign up at aiven.io and create a service: **Apache Kafka** on the **Free** plan, not a trial plan. Trial credits run out after 30 days and the service stops. Wait until it shows **Running**.
2. Open **Topics** → **Create topic**. Name it `link-clicks` (exactly), with **2 partitions**. Auto-creation isn't available on the free plan, so this step is required.
3. Open **Overview** → **Connection information** → **Apache Kafka** tab, and pick authentication method **SASL**:
   - **Service URI** (`kafka-xxx.aivencloud.com:17065`) → `KAFKA_BROKERS`. The SASL port is different from the client-certificate port, so copy it with SASL selected.
   - **User** (`avnadmin`) → `KAFKA_USERNAME`
   - **Password** → `KAFKA_PASSWORD`
   - **CA certificate** → `KAFKA_CA_CERT`. Paste the whole text, including the `-----BEGIN CERTIFICATE-----` and `-----END CERTIFICATE-----` lines. Render accepts multi-line values.

`render.yaml` already sets `KAFKA_TLS=true` and `KAFKA_SASL_MECHANISM=SCRAM-SHA-256`, which is what Aiven uses.

If you prefer client certificates instead: pick **Client certificate**, use that Service URI, and set `KAFKA_CLIENT_CERT` (`service.cert`) and `KAFKA_CLIENT_KEY` (`service.key`) in Render. Delete `KAFKA_SASL_MECHANISM`, `KAFKA_USERNAME` and `KAFKA_PASSWORD`.

## 4. The app on Render

1. Sign up at render.com with your GitHub account, and allow access to the `LinkPulse` repository.
2. Click **New** → **Blueprint**, pick `Harisankar2000/LinkPulse`. Render reads [`render.yaml`](render.yaml).
3. It asks for six secret values: `DATABASE_URL`, `REDIS_URL`, `KAFKA_BROKERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` and `KAFKA_CA_CERT`. Paste them from the steps above, then click **Apply**.
4. The first build takes 3–5 minutes. When it's live, open `https://<your-service>.onrender.com`.

`BASE_URL` is set automatically from Render's public URL (`RENDER_EXTERNAL_URL`), so short links point at the right host.

## 5. Check it works

- `https://<your-service>.onrender.com/readyz` should show `{"postgres":"ok","redis":"ok"}`.
- On the dashboard, shorten a link, click **Simulate 50 clicks**, and the charts should fill within about 10 seconds.
- If the charts stay empty, check Render's **Logs** for `kafka` errors. Usually it's a missing `link-clicks` topic, the client-certificate port used with SASL, a wrong password, or a certificate pasted without its BEGIN/END lines.

## Before an interview

The free Render service sleeps when idle, and Aiven's free Kafka powers off. **Open the site about 5 minutes before** and click through once, so everything is warm. If Kafka was powered off, start it again from the Aiven console.

## How the hosted version differs from `docker compose`

| | Local (`make up`) | Render free |
|---|---|---|
| Analytics consumer | Separate container | Same process (`ANALYTICS_IN_PROCESS=true`), since background workers aren't free |
| Kafka | Plaintext, 6 partitions | TLS + SASL/SCRAM, 2 partitions |
| Country data | Shows `XX` unless you send a header | Shows `XX` unless the request passes through a CDN that adds a country header; the click simulator generates realistic countries |

The code path is identical in both: redirects still publish to Kafka, and the consumer still batches into Postgres rollups.
