import jwt from "jsonwebtoken";

/**
 * =========================
 * TOKEN EXTRACTOR
 * =========================
 */
function getTokenFromHeader(req) {
  const authHeader = req.headers.authorization;

  if (!authHeader) return null;

  const parts = authHeader.split(" ");

  if (parts.length !== 2 || parts[0] !== "Bearer") return null;

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

    // ✅ Normalize user payload (IMPORTANT)
    req.user = {
      id: decoded.id,
      role: decoded.role,
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
 * Use when you want SPECIFIC roles only
 */
export function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized",
      });
    }

    if (!roles.includes(req.user.role)) {
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
 * Central definition of "internal users"
 */
export function isPrivilegedRole(role) {
  return ["super_admin", "admin", "staff"].includes(role);
}

/**
 * =========================
 * PRIVILEGED MIDDLEWARE
 * =========================
 * Use for business operations (vehicles, accounts, users)
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
      message: "Forbidden: insufficient permissions",
    });
  }

  return next();
}