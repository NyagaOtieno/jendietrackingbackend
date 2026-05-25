import {
  redis,
  redisPub
} from "../config/redis.js";

const TTL_SECONDS=60;

export async function setLatestPositionsBatch(
  positions
){

try{

const pipeline=redis.multi();

for(const p of positions){

const key=
`vehicle:${p.deviceId}:latest`;

pipeline.hSet(key,{
lat:p.lat,
lng:p.lon,
speed:p.speed,
heading:p.heading,
timestamp:p.dt.getTime()
});

pipeline.expire(
key,
TTL_SECONDS
);

}

await pipeline.exec();

await redisPub.publish(
"vehicle_updates",
JSON.stringify(
positions
)
);

}
catch(err){

console.error(
"Redis batch failed:",
err.message
);

}

}

export async function getLatestPosition(
deviceId
){

return redis.hGetAll(
`vehicle:${deviceId}:latest`
);

}

export async function getLatestPositionsBulk(
deviceIds
){

const pipeline=
redis.multi();

deviceIds.forEach(id=>{

pipeline.hGetAll(
`vehicle:${id}:latest`
);

});

const results=
await pipeline.exec();

return results.map(
(r,i)=>({

deviceId:deviceIds[i],

...(r[1]||{})

})
);

}