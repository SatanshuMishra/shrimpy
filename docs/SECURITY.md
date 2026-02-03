# Security Guidelines

This document outlines security considerations, implemented mitigations, and deployment best practices for Shrimpy.

## Overview

Shrimpy is a Discord bot with a separate worker process that uses Redis (RQ) for job queuing. The bot processes user-supplied replay files and makes outbound HTTP requests to game APIs.

## Implemented Security Measures

### 1. Redis Security (Critical)

**Risk**: Redis with RQ uses pickle serialization. An exposed Redis instance allows attackers to inject malicious jobs leading to **Remote Code Execution (RCE)**.

**Mitigation implemented**:
- Redis is bound to `127.0.0.1` only (localhost) in `scripts/dev.ps1`
- Password authentication is required via `--requirepass`

**Deployment requirements**:
- **Never expose Redis to the public internet or LAN**
- Use strong, unique passwords (not the default "shrimpy")
- Consider TLS for Redis if running on a remote host
- Use firewall rules to block port 6379/6380 from external access

### 2. HTTP Timeout Protection (High)

**Risk**: Missing timeouts on HTTP requests can cause the bot to hang indefinitely, leading to Denial of Service (DoS).

**Mitigation implemented**:
- All `aiohttp.ClientSession` calls use `ClientTimeout(total=30, connect=10)`
- Applied to: `api/vortex.py`, `api/wg.py`, `bot/extensions/render.py`

### 3. URL Allowlist for Tournament Feature (High)

**Risk**: The tournament integration accepts URLs from message content, which could be exploited for Server-Side Request Forgery (SSRF) or data exfiltration.

**Mitigation implemented**:
- Replay URLs are validated against `ALLOWED_REPLAY_DOMAINS`
- Callback URLs are validated against `ALLOWED_CALLBACK_DOMAINS`
- Blocked requests are logged with a warning

**Allowed domains** (update in `bot/extensions/render.py` if needed):
- Replay URLs: `wows-tournaments.com`, `api.wows-tournaments.com`, `cdn.wows-tournaments.com`, `replays.wows-numbers.com`, `replayswows.com`
- Callback URLs: `wows-tournaments.com`, `api.wows-tournaments.com`

### 4. File Size Limits (High)

**Risk**: Large files can exhaust memory and Redis storage, causing DoS.

**Mitigation implemented**:
- Replay attachments: max 50 MB per file
- Tournament replay downloads: max 50 MB with streaming size check
- Rendered video output: max 25 MB (matches Discord's message limit)
- Oversized renders return a user-friendly error suggesting quality reduction

### 5. Jishaku Disabled in Production (High)

**Risk**: `jishaku` provides powerful debugging commands (eval, shell access) that could be catastrophic if the bot token or owner account is compromised.

**Mitigation implemented**:
- `jishaku` only loads when `ENVIRONMENT != "production"`
- Set `ENVIRONMENT=production` in production deployments

### 6. Stats Persistence Changed from Pickle to JSON (Medium)

**Risk**: `pickle.load()` on untrusted data can lead to arbitrary code execution if an attacker can write to the stats file.

**Mitigation implemented**:
- Stats are now stored in `stats.json` instead of `stats.pickle`
- One-time migration from legacy pickle format is supported
- JSON has no deserialization vulnerabilities

### 7. Privileged Intents (Medium)

**Risk**: The `message_content` intent exposes more user data than necessary.

**Current status**:
- Required for the `guess` game feature and batch render replies
- If you don't need these features, consider disabling the intent

**Best practices**:
- Never log message content
- Have a privacy policy if the bot serves many users
- Minimize data retention

## Deployment Checklist

### Before Production

1. **Environment variable**: Set `ENVIRONMENT=production`
2. **Redis password**: Change from default "shrimpy" to a strong password
3. **secrets.ini**: Ensure file permissions are restricted (`chmod 600` on Linux)
4. **Owner IDs**: Verify `DISCORD_OWNER_IDS` contains only trusted users
5. **Bot token**: Rotate if ever suspected of being leaked

### Infrastructure

1. **Redis**: 
   - Bind to localhost only
   - Never expose to public network
   - Use firewall rules
   - Consider Redis ACLs for finer access control

2. **Worker process**:
   - Run with minimal privileges (non-root)
   - Consider containerization with restricted resources
   - Limit network egress if possible

3. **Logs**:
   - Store in a secure location
   - Set appropriate retention policies
   - Never log tokens or sensitive data

### Monitoring Recommendations

- Alert on unusual Redis memory growth
- Monitor queue length spikes
- Track repeated render failures
- Watch for unexpected outbound requests

## Incident Response

### If the bot token is compromised:

1. Immediately regenerate the token in Discord Developer Portal
2. Update `secrets.ini` with the new token
3. Review audit logs for unauthorized actions
4. Check if any owner accounts were compromised

### If Redis is exposed:

1. Immediately shut down Redis
2. Audit for injected jobs or data
3. Rebuild Redis with localhost-only binding
4. Review worker logs for suspicious activity

## Security Contact

Report security vulnerabilities privately to the repository owner. Do not create public issues for security problems.

## Version History

- **2026-02-03**: Initial security hardening
  - Added HTTP timeouts
  - Added URL allowlists for tournament feature
  - Changed stats persistence from pickle to JSON
  - Added file size limits
  - Gated jishaku behind environment check
  - Bound Redis to localhost only
