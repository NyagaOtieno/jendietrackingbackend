FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY src ./src
COPY services ./services

EXPOSE 4000

CMD ["npm", "start"]