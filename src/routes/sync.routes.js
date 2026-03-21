import express from "express";

const router = express.Router();

router.post("/run", async (_req, res) => {
  console.log("sync route hit");
  return res.status(200).json({
    success: true,
    message: "Direct route test passed",
  });
});

export default router;