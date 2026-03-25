# Implementation Plan: User Authentication for Polymarket Whale Copier

> Generated: 2026-03-22. Not yet implemented.

## Requirements Restatement
- Login page (username/password) with session-based auth (cookie) for browser UI
- JWT Bearer token auth for programmatic API access
- `User` model with bcrypt-hashed passwords in existing SQLite DB
- First admin user seeded from env vars (`ADMIN_USERNAME`, `ADMIN_PASSWORD`)
- All `/api/*` routes (except `/api/auth/login`) require auth → 401 if missing
- All HTML routes (`/`, `/whales`, `/hedge`, `/hedge/whales`) redirect to `/login` if unauthenticated
- Static assets remain public (needed to render login page)

---

## Implementation Phases

### Phase 1: Dependencies & Config
1. Add `python-jose[cryptography]` and `passlib[bcrypt]` to `requirements.txt`
2. Add `SECRET_KEY`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `ACCESS_TOKEN_EXPIRE_HOURS` to `backend/config.py`

### Phase 2: Database Model
3. Add `User` model to `backend/database.py` + `_seed_admin()` called from `init_db()`

### Phase 3: Auth Module (new file)
4. Create `backend/auth.py` with:
   - `verify_password`, `hash_password`
   - `create_access_token`
   - `get_current_user` (cookie → Bearer header fallback, raises 401)
   - `get_current_user_or_redirect` (HTML variant, returns 302 to `/login`)

### Phase 4: Routes & Enforcement
5. Create `frontend/templates/login.html`
6. Add `GET /login`, `POST /api/auth/login`, `POST /api/auth/logout`, `GET /api/auth/me` to `main.py`; apply auth dependencies to **all** existing routes

### Phase 5: Frontend Integration
7. Add 401 redirect handler to `frontend/static/js/app.js`
8. Add login form styles to `frontend/static/css/style.css`

### Phase 6: Docker & Environment
9. Update `.env.example` with new auth vars
10. Verify Dockerfile builds with bcrypt (may need `gcc`/`libffi-dev` on slim image)

---

## Key Risks
| Severity | Risk | Mitigation |
|----------|------|------------|
| HIGH | Missed unprotected route | Use `APIRouter` with default `dependencies=[Depends(get_current_user)]`; write a test that asserts all routes require auth |
| MEDIUM | `SECRET_KEY` not set in prod | Auto-generate at startup + log a prominent warning |
| MEDIUM | bcrypt build failure in Docker | `passlib[bcrypt]` wheels available; fallback: add `gcc`/`libffi-dev` to Dockerfile |
| MEDIUM | Expired session breaks JS polling | 401 redirect wrapper in `app.js` handles this |

---

## Files to Change
| File | Change |
|------|--------|
| `requirements.txt` | Add `python-jose[cryptography]`, `passlib[bcrypt]` |
| `backend/config.py` | Add auth settings |
| `backend/database.py` | Add `User` model + `_seed_admin()` |
| `backend/auth.py` | **New** — hashing, JWT, FastAPI dependencies |
| `backend/main.py` | Add auth routes, protect all existing routes |
| `frontend/templates/login.html` | **New** — login form |
| `frontend/static/js/app.js` | Add 401 redirect handler |
| `frontend/static/css/style.css` | Login form styles |
| `.env.example` | Document new auth env vars |

---

## Success Criteria
- [ ] Unauthenticated requests to any `/api/*` endpoint (except `/api/auth/login`) return HTTP 401
- [ ] Unauthenticated requests to any HTML page redirect to `/login`
- [ ] Login page renders correctly and accepts username/password
- [ ] Successful login sets an HttpOnly cookie and redirects to `/`
- [ ] All existing dashboard functionality works identically after login
- [ ] Logout clears the cookie and redirects to `/login`
- [ ] API clients can authenticate via `Authorization: Bearer <token>` header
- [ ] First admin user seeded from `ADMIN_USERNAME` / `ADMIN_PASSWORD` env vars on first run
- [ ] Docker build succeeds with new dependencies
- [ ] Static assets (`/static/*`) accessible without authentication
