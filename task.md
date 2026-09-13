# Bank Live Ranking Feature — Task Checklist

## Backend — Database & Models
- [x] Add `BankLiveRankingConfig` model to `models_quotation.py`
- [x] Add `entity_id` column to `QuotationRequest`
- [x] Run migration (table created, column added)
- [x] Add schemas (`BankLiveRankingConfigCreate`, `BankLiveRankingConfigUpdate`, `BankLiveRankingConfigOut`)

## Backend — Services
- [x] Create `live_ranking_service.py` (eligibility check + rank calculation)

## Backend — System Owner CRUD Endpoints
- [x] `GET /live-ranking-configs` (list with filters)
- [x] `POST /live-ranking-configs` (create/upsert)
- [x] `PUT /live-ranking-configs/{id}` (update)
- [x] `DELETE /live-ranking-configs/{id}` (delete)
- [x] `GET /customers/{customer_id}/entities` (entity dropdown)

## Backend — Public Quotations Integration
- [x] Modify `get_rfq_by_token` — add `is_live_ranking_enabled` + `live_rank`
- [x] Modify `submit_fx_offer` — return `live_rank` in response
- [x] Modify `submit_tbill_offer` — return `live_rank` in response
- [x] Add `GET /{token}/live-rank` polling endpoint

## Frontend — System Owner Management Page
- [x] Create `QuotationLiveRankingManagement.js`
- [x] Add route + nav link in System Owner layout (`SystemOwnerRoutes.js` + `SidebarLayout.js`)

## Frontend — Bank Quoting Portal
- [x] Modify `QuotationBankOfferPage.js` to show Live Rank HUD
- [x] Auto-polling rank every 4 seconds while window open

## Verification
- [x] Python files compile
- [x] Backend logic test script passes
- [/] Frontend build verification
