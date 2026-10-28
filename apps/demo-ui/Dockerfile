FROM node:22-alpine AS build
WORKDIR /app

COPY package.json tsconfig.base.json ./
COPY apps/demo-ui ./apps/demo-ui

RUN npm install --no-audit --no-fund
RUN npm run build -w @poc/demo-ui

FROM nginx:1.27-alpine
COPY apps/demo-ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/apps/demo-ui/dist /usr/share/nginx/html
EXPOSE 80
