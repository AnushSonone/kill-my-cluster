# Demo frontend (SvelteKit) — proxies /api to the control plane.

FROM node:22-bookworm AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=5173
ENV HOST=0.0.0.0
ENV CONTROLPLANE_URL=http://controlplane:8080
COPY --from=build /app/build ./build
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
EXPOSE 5173
CMD ["node", "build"]
