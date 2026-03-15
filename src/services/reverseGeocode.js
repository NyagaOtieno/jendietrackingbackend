import axios from "axios";

export async function getLocationName(lat, lon) {
  if (!lat || !lon) return "Unknown location";

  if (process.env.ENABLE_REVERSE_GEOCODE !== "true") {
    return "Unknown location";
  }

  try {
    const { data } = await axios.get(
      "https://nominatim.openstreetmap.org/reverse",
      {
        params: {
          format: "jsonv2",
          lat,
          lon,
        },
        headers: {
          "User-Agent": "jendie-tracking-backend/1.0",
        },
        timeout: 10000,
      }
    );

    return (
      data?.display_name ||
      data?.name ||
      data?.address?.road ||
      "Unknown location"
    );
  } catch {
    return "Unknown location";
  }
}