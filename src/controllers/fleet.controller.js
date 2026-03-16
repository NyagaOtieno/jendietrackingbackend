export async function getFleets(_req, res) {
  try {
    const result = await query(`
      SELECT id, name, created_at
      FROM fleets
      ORDER BY created_at DESC
    `);

    return res.json({
      success: true,
      data: result.rows,
    });
  } catch (error) {
    console.error("getFleets error:", error);
    res.status(500).json({ success: false, message: "Failed to load fleets" });
  }
}

export async function getFleetById(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `SELECT * FROM fleets WHERE id = $1`,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Fleet not found",
      });
    }

    res.json({ success: true, data: result.rows[0] });
  } catch (error) {
    console.error("getFleetById error:", error);
    res.status(500).json({ success: false, message: "Failed to load fleet" });
  }
}

export async function createFleet(req, res) {
  try {
    const { name } = req.body;

    const result = await query(
      `INSERT INTO fleets (name)
       VALUES ($1)
       RETURNING *`,
      [name]
    );

    res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("createFleet error:", error);
    res.status(500).json({ success: false, message: "Failed to create fleet" });
  }
}

export async function updateFleet(req, res) {
  try {
    const { id } = req.params;
    const { name } = req.body;

    const result = await query(
      `UPDATE fleets
       SET name = $1
       WHERE id = $2
       RETURNING *`,
      [name, id]
    );

    res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("updateFleet error:", error);
    res.status(500).json({ success: false, message: "Failed to update fleet" });
  }
}

export async function deleteFleet(req, res) {
  try {
    const { id } = req.params;

    await query(`DELETE FROM fleets WHERE id = $1`, [id]);

    res.json({
      success: true,
      message: "Fleet deleted",
    });
  } catch (error) {
    console.error("deleteFleet error:", error);
    res.status(500).json({ success: false, message: "Failed to delete fleet" });
  }
}