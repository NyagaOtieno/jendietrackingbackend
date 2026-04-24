import jwt from "jsonwebtoken";

/**
 * =========================
 * TOKEN EXTRACTOR (ROBUST)
 * =========================
 */
function getTokenFromHeader(req) {
  const authHeader = req.headers.authorization || "";

  if (!authHeader) return null;

  const parts = authHeader.trim().split(" ");

  if (parts.length !== 2) return null;
  if (parts[0].toLowerCase() !== "bearer") return null;

  return parts[1];
}

/**
 * =========================
 * AUTH MIDDLEWARE
 * =========================
 */
export function requireAuth(req, res, next) {
  try {
    const token = getTokenFromHeader(req);

    if (!token) {
      return res.status(401).json({
        success: false,
        message: "Authorization token is required",
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // ✅ Normalize + sanitize role (CRITICAL FIX)
    const role = (decoded.role || "").trim().toLowerCase();

    req.user = {
      id: decoded.id,
      role,
      accountId: decoded.accountId || null,
    };

    return next();
  } catch (error) {
    return res.status(401).json({
      success: false,
      message: "Invalid or expired token",
    });
  }
}

/**
 * =========================
 * ROLE CHECK (STRICT)
 * =========================
 */
export function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized",
      });
    }

    const role = (req.user.role || "").trim().toLowerCase();

    const allowed = roles.map(r => r.toLowerCase());

    if (!allowed.includes(role)) {
      return res.status(403).json({
        success: false,
        message: "Forbidden: insufficient permissions",
      });
    }

    return next();
  };
}

/**
 * =========================
 * PRIVILEGED ROLE CHECK
 * =========================
 */
export function isPrivilegedRole(role) {
  const r = (role || "").trim().toLowerCase();

  return ["super_admin", "admin", "staff"].includes(r);
}

/**
 * =========================
 * PRIVILEGED MIDDLEWARE
 * =========================
 */
export function requirePrivileged(req, res, next) {
  if (!req.user) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized",
    });
  }

  if (!isPrivilegedRole(req.user.role)) {
    return res.status(403).json({
      success: false,
      message: "Forbidden: client role not allowed",
    });
  }

  return next();
}