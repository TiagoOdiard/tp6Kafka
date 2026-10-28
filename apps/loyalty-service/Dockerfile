FROM node:22-alpine AS build
WORKDIR /app

COPY package.json tsconfig.base.json ./
COPY libs ./libs
COPY apps/loyalty-service ./apps/loyalty-service

RUN npm install --no-audit --no-fund
RUN npm run build -w @poc/contracts \
 && npm run build -w @poc/kafka-toolkit \
 && npm run build -w @poc/loyalty-service
RUN npm prune --omit=dev

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/libs/contracts/package.json ./libs/contracts/package.json
COPY --from=build /app/libs/contracts/dist ./libs/contracts/dist
COPY --from=build /app/libs/kafka-toolkit/package.json ./libs/kafka-toolkit/package.json
COPY --from=build /app/libs/kafka-toolkit/dist ./libs/kafka-toolkit/dist
COPY --from=build /app/apps/loyalty-service/package.json ./apps/loyalty-service/package.json
COPY --from=build /app/apps/loyalty-service/dist ./apps/loyalty-service/dist

CMD ["node", "apps/loyalty-service/dist/main.js"]
