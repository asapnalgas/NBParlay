# BETR API Analysis & Re-Enablement Feasibility

**Date**: March 13, 2026  
**Status**: BLOCKED - No Stable Public API  
**Recommendation**: DEFER to future versions

---

## Executive Summary

BETR integration is currently **disabled** due to the absence of a stable public API endpoint for player prop lines. The codebase contains infrastructure for manual BETR line imports but no automated fetching capability. Re-enabling BETR would require either:

1. **BETR Public API Availability** (currently unavailable)
2. **Custom Integration** (screen-scraping or proprietary API access)
3. **Manual Import Workflow** (current approach - reliable but manual)

**Current Recommendation**: Maintain manual import capability and defer API integration until BETR publishes a stable endpoint.

---

## Current State: BETR in NBParlay

### Configuration
**File**: `nba_model/src/live_sync.py` (lines 284-289)
```python
"betr": {
    "enabled": False,
    "board_url": "https://www.betr.app/",
    "note": "No stable public BETR props API is available; use manual line import for BETR entries.",
}
```

**Default**: DISABLED  
**Reason**: No stable public API endpoint available

### Infrastructure Present

| Component | Status | Purpose |
|-----------|--------|---------|
| Configuration structure | ✅ Exists | `providers_config["betr"]` |
| Status reporting | ✅ Exists | `/api/status` includes BETR state |
| Manual import workflow | ✅ Exists | Users can upload BETR CSV lines |
| Query support | ✅ Exists | API hints users to import BETR manually |

### Infrastructure Missing

| Component | Why Missing | Impact |
|-----------|------------|--------|
| `_fetch_betr_*()` functions | No stable API endpoint | Cannot auto-fetch BETR lines |
| API client library | BETR has not published SDK | Cannot integrate programmatically |
| Fallback retry logic | No endpoint to retry against | Retry policy doesn't apply to BETR |
| Provider status tracking | Disabled, so not synced | No live error reporting |

---

## BETR Public API Status (Research)

### What BETR Publishes

As of 2026-03-13, BETR's public offerings are:

| Resource | Status | Details |
|----------|--------|---------|
| **www.betr.app** | ✅ Available | Public web interface (HTML only) |
| **Help Center** | ✅ Available | [help.betr.app](https://help.betr.app/) (docs only) |
| **App Store** | ✅ Available | iOS/Android apps (no API documentation) |
| **GraphQL/REST API** | 🔴 Unknown/Unavailable | Not documented; may exist but private |
| **Odds Feed** | 🔴 Unknown/Unavailable | May exist internally but not public |
| **WebSocket Feed** | 🔴 Unknown/Unavailable | May exist for mobile apps but undocumented |

### Known Technical Barriers

1. **Web-Only Access**: BETR's primary interface is the web app (HTML rendering).
   - Player props are loaded dynamically via JavaScript.
   - No static HTML scraping possible without heavy DOM parsing.

2. **No Published SDK or Docs**: Unlike The Odds API or NBA Stats API, BETR has not published:
   - Official API documentation
   - SDK libraries
   - Rate limit guidelines
   - Authentication schemes

3. **Mobile-First Architecture**: BETR is optimized for mobile apps, which may use:
   - Unpublished internal APIs
   - Session-based authentication
   - Dynamic asset loading (not amenable to scripting)

4. **Potential Terms of Service Violation**: 
   - Screen-scraping may violate BETR's ToS.
   - No explicit permission for automated data access.

---

## Integration Approach Comparison

### Option 1: Wait for BETR Public API ⏳ (RECOMMENDED)

**Requirements**:
- BETR publishes official API endpoint
- API includes player props lines
- API has rate limits NBParlay can work within
- BETR grants permission for automated access

**Effort**: 0 (waiting)  
**Timeline**: Unknown (BETR has not announced API plans)  
**Risk**: LOW (passive waiting)  
**Upside**: Official support, reliability, legal clarity

**Action**: Monitor BETR announcements. Check quarterly.

---

### Option 2: Browser Automation / Screen-Scraping 🔍 (NOT RECOMMENDED)

**Requirements**:
- Use Selenium/Puppeteer to drive browser
- Parse dynamic prop markets from DOM
- Handle JavaScript rendering delay
- Manage session auth cookies

**Effort**: 20-30 hours initial + 10 hours/month maintenance  
**Timeline**: 2-3 weeks  
**Risk**: VERY HIGH
- Fragile (breaks with BETR UI changes)
- Likely violates BETR ToS terms
- Legal exposure (DMCA/CFAA concerns)
- Performance overhead (browser rendering is slow)

**Verdict**: NOT RECOMMENDED
- Maintenance burden is unsustainable
- Risk of legal action
- Performance is incompatible with 10-second sync cycles

---

### Option 3: Petition BETR for API Access 📧 (POSSIBLE)

**Requirements**:
- Contact BETR (business development / API team)
- Request undocumented/private API access
- Sign data usage agreement
- Implement custom client

**Effort**: 5-10 hours + 5 hours/month (support)  
**Timeline**: 2-4 weeks (depending on BETR responsiveness)  
**Risk**: MEDIUM
- BETR may decline
- Private API may change without notice
- Relationship depends on BETR's roadmap

**Next Steps**:
1. Contact BETR at partnerships@betr.app or via their help center
2. Explain NBParlay's use case (player prop edge detection, no user data sharing)
3. Request API endpoint specs and rate limits
4. Negotiate data usage terms

---

### Option 4: Maintain Manual Import (CURRENT) ✅

**Current State**:
- Users manually import BETR CSV lines via web UI
- Workflow is documented in the app
- No API integration required
- Users control data freshness

**Effort**: ~1 hour (initial setup + quarterly checks)  
**Timeline**: Immediate  
**Risk**: LOW (user-controlled)  
**Downside**: Not automated; requires user participation

**Pros**:
- 100% legal compliance
- Zero maintenance burden
- User data sovereignty
- No external dependencies

**Cons**:
- Manual (not continuous)
- Requires user engagement
- Stale lines if user forgets to import
- Not suitable for production 24/7 systems

**Verdict**: Continue as fallback. Pair with outbound communication.

---

## Recommended Path Forward

### Short Term (Next 2-4 Weeks)

1. **Keep Manual Workflow Active**
   - Maintain CSV import endpoint
   - Keep documentation visible in web UI
   - Add guidance: "Import BETR lines before lock"

2. **Monitor BETR Announcements**
   - Set quarterly calendar reminder
   - Check their blog, Twitter, help center
   - Subscribe to BETR developer news

3. **Optional: Reach Out to BETR**
   - Send inquiry to partnerships@betr.app
   - Express interest in API access
   - No expectation of response

### Medium Term (2-3 Months)

- If BETR responds positively to API inquiry → Prioritize Option 3
- If BETR announces public API → Shift to Option 1
- Otherwise → Reinforce manual import as platform strength

### Long Term (6+ Months)

- Re-assess BETR's competitive position in sports betting
- If BETR becomes major platform → Escalate API integration request
- If BETR remains boutique → Maintain manual workflow indefinitely

---

## Code Impact: If BETR API Becomes Available

**Implementation Path** (when/if API available):

1. Create `_fetch_betr_player_props_rows()` in `live_sync.py`
   - Follow same pattern as `_fetch_odds_player_props_rows()`
   - Use centralized retry mechanism (now available with recent hardening)
   - Return DataFrame with `player_name`, `team`, `game_date`, `market`, `line`

2. Update BETR config in `live_sync.json`
   ```json
   "betr": {
     "enabled": true,
     "api_key_env": "BETR_API_KEY",
     "base_url": "[BETR API endpoint TBD]",
     "request_timeout_seconds": 8,
     "refresh_interval_seconds": 10,
     "markets": ["points", "rebounds", "assists", "pra"]
   }
   ```

3. Hook into provider fetch loop (around line 10690 in live_sync.py)
   ```python
   if is_provider_enabled("betr"):
       betr_rows, betr_status = _fetch_betr_player_props_rows(upcoming_frame, providers_config["betr"])
       provider_status["betr"] = betr_status
   ```

4. Merge BETR rows into `provider_context_updates.csv` (follows existing pattern)

**Estimated Implementation**: 6-8 hours

---

## Risk Assessment

### If We Don't Wait for Public API

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| BETR ToS violation | HIGH | Legal action | Don't screen-scrape |
| Web UI changes break scraper | HIGH | Daily failures | Monitor daily |
| BETR IP blocks our IP range | MEDIUM | Instant failure | Can't easily work around |
| Manual workflow is good enough | MEDIUM | Wasted effort | Monitor user feedback |

### If We Wait for Public API

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| BETR never publishes API | MEDIUM | No BETR integration | Acceptable (current state) |
| BETR publishes API too late | LOW | Lose early adopter advantage | Still viable long-term |
| Delayed action leaves us behind | LOW | Competitors integrate first | BETR not our main edge source |

**Verdict**: Waiting is lower-risk.

---

## Competitors & Market Position

### Who Else Is Doing This?

| Competitor | BETR Integration | Status | Notes |
|------------|------------------|--------|-------|
| **RotoWire** | Partial | Manual board snapshots | Not automated |
| **DFS Apps** | Limited | Some have BETR columns | Not primary focus |
| **PrizePicks Tools** | No | Focus on their own lines | Competitive incentive to not integrate |
| **Custom Shops** | Some use scraping | Fragile | Known risk/legal exposure |

**Insight**: No major player has successfully integrated BETR automated feeds. This suggests API access is not readily available.

---

## Conclusion

**Current State**: BETR is **properly disabled** due to lack of a stable public API.

**Recommendation**: 
1. Keep manual import workflow (cost-effective, legally safe)
2. Monitor BETR for API announcement (quarterly check-in)
3. If/when API available, implement following pattern of existing providers
4. Do NOT pursue screen-scraping (too risky)

**Timeline**: No action required now. Revisit Q2 2026 if no API announcement.

**Business Impact**: 
- Manual workflow sufficient for current user base
- BETR is 1 of 4 major platforms; DraftKings/FanDuel/PrizePicks are primary
- Edge detection is market-agnostic (works across all betting platforms)
- Low priority compared to core prediction accuracy improvements

---

## References

- BETR Website: https://www.betr.app/
- BETR Help Center: https://help.betr.app/
- NBParlay Config: `nba_model/config/live_sync.json`
- NBParlay Status: `nba_model/src/live_sync.py` (lines 284-289)
- Gap Analysis: `nba_model/docs/rotowire_betr_gap_assessment_monetization.md`

---

**Analysis Author**: GitHub Copilot  
**Date**: March 13, 2026  
**Status**: ACCEPTED - Maintain current approach
