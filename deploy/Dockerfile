# bridged — one Node process, no native modules (node:sqlite is built in).
#
# `--ignore-scripts`: nothing in the dependency tree needs a postinstall, and
# a lockfile install that cannot run scripts cannot run a worm either.
FROM node:22-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY lib ./lib
COPY bin ./bin
ENV NODE_ENV=production
USER node
EXPOSE 8080
ENTRYPOINT ["node", "bin/bridged.cjs"]
CMD ["serve"]
