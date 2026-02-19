-- MetalLedger v0 — Synthetic Seed Data
-- 30+ rows per metal spanning ~6 weeks (2024-01-02 to 2024-02-09)
-- XAU: $1900–2100/oz | XAG: $22–26/oz | CU: $3.60–4.20/lb

-- ============================================================
-- XAU — Gold (troy oz, USD)
-- ============================================================
INSERT INTO prices_raw (source, metal, venue, price_ts, value, currency, source_id) VALUES
('seed', 'XAU', 'SPOT', '2024-01-02 20:00:00+00', 2063.10, 'USD', 'seed-xau-001'),
('seed', 'XAU', 'SPOT', '2024-01-03 20:00:00+00', 2047.30, 'USD', 'seed-xau-002'),
('seed', 'XAU', 'SPOT', '2024-01-04 20:00:00+00', 2031.55, 'USD', 'seed-xau-003'),
('seed', 'XAU', 'SPOT', '2024-01-05 20:00:00+00', 2024.80, 'USD', 'seed-xau-004'),
('seed', 'XAU', 'SPOT', '2024-01-08 20:00:00+00', 2029.45, 'USD', 'seed-xau-005'),
('seed', 'XAU', 'SPOT', '2024-01-09 20:00:00+00', 2038.70, 'USD', 'seed-xau-006'),
('seed', 'XAU', 'SPOT', '2024-01-10 20:00:00+00', 2018.60, 'USD', 'seed-xau-007'),
('seed', 'XAU', 'SPOT', '2024-01-11 20:00:00+00', 2005.20, 'USD', 'seed-xau-008'),
('seed', 'XAU', 'SPOT', '2024-01-12 20:00:00+00', 2012.75, 'USD', 'seed-xau-009'),
('seed', 'XAU', 'SPOT', '2024-01-16 20:00:00+00', 2029.90, 'USD', 'seed-xau-010'),
('seed', 'XAU', 'SPOT', '2024-01-17 20:00:00+00', 2007.45, 'USD', 'seed-xau-011'),
('seed', 'XAU', 'SPOT', '2024-01-18 20:00:00+00', 1998.30, 'USD', 'seed-xau-012'),
('seed', 'XAU', 'SPOT', '2024-01-19 20:00:00+00', 2014.60, 'USD', 'seed-xau-013'),
('seed', 'XAU', 'SPOT', '2024-01-22 20:00:00+00', 2022.15, 'USD', 'seed-xau-014'),
('seed', 'XAU', 'SPOT', '2024-01-23 20:00:00+00', 2018.80, 'USD', 'seed-xau-015'),
('seed', 'XAU', 'SPOT', '2024-01-24 20:00:00+00', 2031.95, 'USD', 'seed-xau-016'),
('seed', 'XAU', 'SPOT', '2024-01-25 20:00:00+00', 2044.70, 'USD', 'seed-xau-017'),
('seed', 'XAU', 'SPOT', '2024-01-26 20:00:00+00', 2018.50, 'USD', 'seed-xau-018'),
('seed', 'XAU', 'SPOT', '2024-01-29 20:00:00+00', 2026.85, 'USD', 'seed-xau-019'),
('seed', 'XAU', 'SPOT', '2024-01-30 20:00:00+00', 2033.40, 'USD', 'seed-xau-020'),
('seed', 'XAU', 'SPOT', '2024-01-31 20:00:00+00', 2039.75, 'USD', 'seed-xau-021'),
('seed', 'XAU', 'SPOT', '2024-02-01 20:00:00+00', 2055.20, 'USD', 'seed-xau-022'),
('seed', 'XAU', 'SPOT', '2024-02-02 20:00:00+00', 2038.60, 'USD', 'seed-xau-023'),
('seed', 'XAU', 'SPOT', '2024-02-05 20:00:00+00', 2045.30, 'USD', 'seed-xau-024'),
('seed', 'XAU', 'SPOT', '2024-02-06 20:00:00+00', 2052.90, 'USD', 'seed-xau-025'),
('seed', 'XAU', 'SPOT', '2024-02-07 20:00:00+00', 2061.40, 'USD', 'seed-xau-026'),
('seed', 'XAU', 'SPOT', '2024-02-08 20:00:00+00', 2048.75, 'USD', 'seed-xau-027'),
('seed', 'XAU', 'SPOT', '2024-02-09 20:00:00+00', 2056.20, 'USD', 'seed-xau-028'),
('seed', 'XAU', 'SPOT', '2024-02-12 20:00:00+00', 2001.85, 'USD', 'seed-xau-029'),
('seed', 'XAU', 'SPOT', '2024-02-13 20:00:00+00', 1991.40, 'USD', 'seed-xau-030'),
('seed', 'XAU', 'SPOT', '2024-02-14 20:00:00+00', 2005.60, 'USD', 'seed-xau-031'),
('seed', 'XAU', 'SPOT', '2024-02-15 20:00:00+00', 1982.30, 'USD', 'seed-xau-032');

-- ============================================================
-- XAG — Silver (troy oz, USD)
-- ============================================================
INSERT INTO prices_raw (source, metal, venue, price_ts, value, currency, source_id) VALUES
('seed', 'XAG', 'SPOT', '2024-01-02 20:00:00+00', 23.45, 'USD', 'seed-xag-001'),
('seed', 'XAG', 'SPOT', '2024-01-03 20:00:00+00', 23.18, 'USD', 'seed-xag-002'),
('seed', 'XAG', 'SPOT', '2024-01-04 20:00:00+00', 22.89, 'USD', 'seed-xag-003'),
('seed', 'XAG', 'SPOT', '2024-01-05 20:00:00+00', 22.71, 'USD', 'seed-xag-004'),
('seed', 'XAG', 'SPOT', '2024-01-08 20:00:00+00', 22.94, 'USD', 'seed-xag-005'),
('seed', 'XAG', 'SPOT', '2024-01-09 20:00:00+00', 23.22, 'USD', 'seed-xag-006'),
('seed', 'XAG', 'SPOT', '2024-01-10 20:00:00+00', 23.05, 'USD', 'seed-xag-007'),
('seed', 'XAG', 'SPOT', '2024-01-11 20:00:00+00', 22.63, 'USD', 'seed-xag-008'),
('seed', 'XAG', 'SPOT', '2024-01-12 20:00:00+00', 22.80, 'USD', 'seed-xag-009'),
('seed', 'XAG', 'SPOT', '2024-01-16 20:00:00+00', 23.14, 'USD', 'seed-xag-010'),
('seed', 'XAG', 'SPOT', '2024-01-17 20:00:00+00', 22.47, 'USD', 'seed-xag-011'),
('seed', 'XAG', 'SPOT', '2024-01-18 20:00:00+00', 22.32, 'USD', 'seed-xag-012'),
('seed', 'XAG', 'SPOT', '2024-01-19 20:00:00+00', 22.55, 'USD', 'seed-xag-013'),
('seed', 'XAG', 'SPOT', '2024-01-22 20:00:00+00', 22.78, 'USD', 'seed-xag-014'),
('seed', 'XAG', 'SPOT', '2024-01-23 20:00:00+00', 23.01, 'USD', 'seed-xag-015'),
('seed', 'XAG', 'SPOT', '2024-01-24 20:00:00+00', 23.28, 'USD', 'seed-xag-016'),
('seed', 'XAG', 'SPOT', '2024-01-25 20:00:00+00', 23.67, 'USD', 'seed-xag-017'),
('seed', 'XAG', 'SPOT', '2024-01-26 20:00:00+00', 23.44, 'USD', 'seed-xag-018'),
('seed', 'XAG', 'SPOT', '2024-01-29 20:00:00+00', 23.55, 'USD', 'seed-xag-019'),
('seed', 'XAG', 'SPOT', '2024-01-30 20:00:00+00', 23.71, 'USD', 'seed-xag-020'),
('seed', 'XAG', 'SPOT', '2024-01-31 20:00:00+00', 23.89, 'USD', 'seed-xag-021'),
('seed', 'XAG', 'SPOT', '2024-02-01 20:00:00+00', 24.12, 'USD', 'seed-xag-022'),
('seed', 'XAG', 'SPOT', '2024-02-02 20:00:00+00', 23.98, 'USD', 'seed-xag-023'),
('seed', 'XAG', 'SPOT', '2024-02-05 20:00:00+00', 24.21, 'USD', 'seed-xag-024'),
('seed', 'XAG', 'SPOT', '2024-02-06 20:00:00+00', 24.45, 'USD', 'seed-xag-025'),
('seed', 'XAG', 'SPOT', '2024-02-07 20:00:00+00', 24.68, 'USD', 'seed-xag-026'),
('seed', 'XAG', 'SPOT', '2024-02-08 20:00:00+00', 24.33, 'USD', 'seed-xag-027'),
('seed', 'XAG', 'SPOT', '2024-02-09 20:00:00+00', 24.55, 'USD', 'seed-xag-028'),
('seed', 'XAG', 'SPOT', '2024-02-12 20:00:00+00', 23.87, 'USD', 'seed-xag-029'),
('seed', 'XAG', 'SPOT', '2024-02-13 20:00:00+00', 23.61, 'USD', 'seed-xag-030'),
('seed', 'XAG', 'SPOT', '2024-02-14 20:00:00+00', 23.78, 'USD', 'seed-xag-031'),
('seed', 'XAG', 'SPOT', '2024-02-15 20:00:00+00', 23.42, 'USD', 'seed-xag-032');

-- ============================================================
-- CU — Copper (lb, USD)
-- ============================================================
INSERT INTO prices_raw (source, metal, venue, price_ts, value, currency, source_id) VALUES
('seed', 'CU', 'SPOT', '2024-01-02 20:00:00+00', 3.892, 'USD', 'seed-cu-001'),
('seed', 'CU', 'SPOT', '2024-01-03 20:00:00+00', 3.871, 'USD', 'seed-cu-002'),
('seed', 'CU', 'SPOT', '2024-01-04 20:00:00+00', 3.845, 'USD', 'seed-cu-003'),
('seed', 'CU', 'SPOT', '2024-01-05 20:00:00+00', 3.812, 'USD', 'seed-cu-004'),
('seed', 'CU', 'SPOT', '2024-01-08 20:00:00+00', 3.829, 'USD', 'seed-cu-005'),
('seed', 'CU', 'SPOT', '2024-01-09 20:00:00+00', 3.857, 'USD', 'seed-cu-006'),
('seed', 'CU', 'SPOT', '2024-01-10 20:00:00+00', 3.841, 'USD', 'seed-cu-007'),
('seed', 'CU', 'SPOT', '2024-01-11 20:00:00+00', 3.798, 'USD', 'seed-cu-008'),
('seed', 'CU', 'SPOT', '2024-01-12 20:00:00+00', 3.815, 'USD', 'seed-cu-009'),
('seed', 'CU', 'SPOT', '2024-01-16 20:00:00+00', 3.834, 'USD', 'seed-cu-010'),
('seed', 'CU', 'SPOT', '2024-01-17 20:00:00+00', 3.762, 'USD', 'seed-cu-011'),
('seed', 'CU', 'SPOT', '2024-01-18 20:00:00+00', 3.741, 'USD', 'seed-cu-012'),
('seed', 'CU', 'SPOT', '2024-01-19 20:00:00+00', 3.778, 'USD', 'seed-cu-013'),
('seed', 'CU', 'SPOT', '2024-01-22 20:00:00+00', 3.804, 'USD', 'seed-cu-014'),
('seed', 'CU', 'SPOT', '2024-01-23 20:00:00+00', 3.823, 'USD', 'seed-cu-015'),
('seed', 'CU', 'SPOT', '2024-01-24 20:00:00+00', 3.856, 'USD', 'seed-cu-016'),
('seed', 'CU', 'SPOT', '2024-01-25 20:00:00+00', 3.901, 'USD', 'seed-cu-017'),
('seed', 'CU', 'SPOT', '2024-01-26 20:00:00+00', 3.878, 'USD', 'seed-cu-018'),
('seed', 'CU', 'SPOT', '2024-01-29 20:00:00+00', 3.912, 'USD', 'seed-cu-019'),
('seed', 'CU', 'SPOT', '2024-01-30 20:00:00+00', 3.944, 'USD', 'seed-cu-020'),
('seed', 'CU', 'SPOT', '2024-01-31 20:00:00+00', 3.967, 'USD', 'seed-cu-021'),
('seed', 'CU', 'SPOT', '2024-02-01 20:00:00+00', 3.999, 'USD', 'seed-cu-022'),
('seed', 'CU', 'SPOT', '2024-02-02 20:00:00+00', 3.981, 'USD', 'seed-cu-023'),
('seed', 'CU', 'SPOT', '2024-02-05 20:00:00+00', 4.012, 'USD', 'seed-cu-024'),
('seed', 'CU', 'SPOT', '2024-02-06 20:00:00+00', 4.045, 'USD', 'seed-cu-025'),
('seed', 'CU', 'SPOT', '2024-02-07 20:00:00+00', 4.078, 'USD', 'seed-cu-026'),
('seed', 'CU', 'SPOT', '2024-02-08 20:00:00+00', 4.056, 'USD', 'seed-cu-027'),
('seed', 'CU', 'SPOT', '2024-02-09 20:00:00+00', 4.089, 'USD', 'seed-cu-028'),
('seed', 'CU', 'SPOT', '2024-02-12 20:00:00+00', 3.998, 'USD', 'seed-cu-029'),
('seed', 'CU', 'SPOT', '2024-02-13 20:00:00+00', 3.965, 'USD', 'seed-cu-030'),
('seed', 'CU', 'SPOT', '2024-02-14 20:00:00+00', 3.987, 'USD', 'seed-cu-031'),
('seed', 'CU', 'SPOT', '2024-02-15 20:00:00+00', 3.942, 'USD', 'seed-cu-032');

-- ============================================================
-- Promote seed data to canonical (all pass outlier checks)
-- ============================================================
INSERT INTO prices_canonical (metal, price_ts, value, currency, source, raw_id)
SELECT metal, price_ts, value, currency, source, id
FROM prices_raw
WHERE source = 'seed'
ON CONFLICT (metal, price_ts) DO NOTHING;
