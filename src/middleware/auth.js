import jwt from "jsonwebtoken";

function getTokenFromHeader(req) {
  const authHeader = req.headers.authorization || "";

  if (!authHeader.startsWith("Bearer ")) return null;

  return authHeader.slice(7).trim();
}

// ======================================================
// AUTH MIDDLEWARE
// ======================================================
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

    // 🔥 normalize user payload (IMPORTANT FIX)
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

// ======================================================
// ROLE CHECK
// ======================================================
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

// ======================================================
// PRIVILEGED ROLES (FIXED CONSISTENCY)
// ======================================================
export function isPrivilegedRole(role) {
  return ["super_admin", "admin", "staff"].includes(role);
}