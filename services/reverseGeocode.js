import axios from "axios";

const locationCache = new Map();

export const getLocationName = async (lat, lon) => {
  const latNum = Number(lat);
  const lonNum = Number(lon);

  if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) {
    return null;
  }

  const key = `${latNum.toFixed(4)},${lonNum.toFixed(4)}`;

  if (locationCache.has(key)) {
    return locationCache.get(key);
  }

  try {
    const response = await axios.get(
      "https://nominatim.openstreetmap.org/reverse",
      {
        params: {
          lat: latNum,
          lon: lonNum,
          format: "json"
        },
        headers: {
          "User-Agent": "JendieTrackingPlatform"
        }
      }
    );

    const name = response.data?.display_name || null;

    locationCache.set(key, name);

    return name;

  } catch (error) {
    console.error("Reverse geocode failed:", error.message);
    return null;
  }
};