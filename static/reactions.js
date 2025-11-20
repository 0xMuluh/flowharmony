(function () {
  const registry = new Map();

  function normalizeChannel(channel) {
    return channel || "default";
  }

  function mount(options) {
    if (!options) {
      return;
    }
    const channel = normalizeChannel(options.channel);
    const container = options.container || (options.containerId ? document.getElementById(options.containerId) : null);
    if (!container) {
      return;
    }
    const maxVisible = Number(options.maxVisible || 4);
    const durationMs = Number(options.durationMs || 4800);
    const ctx = registry.get(channel) || {
      container,
      maxVisible,
      durationMs,
      seen: new Map(),
    };
    ctx.container = container;
    ctx.maxVisible = maxVisible;
    ctx.durationMs = durationMs;
    if (!registry.has(channel)) {
      registry.set(channel, ctx);
    }
    return ctx;
  }

  function pruneSeen(ctx) {
    const MAX_SEEN = 320;
    if (ctx.seen.size <= MAX_SEEN) {
      return;
    }
    const entries = Array.from(ctx.seen.entries()).sort((a, b) => a[1] - b[1]);
    const trimCount = ctx.seen.size - MAX_SEEN;
    for (let i = 0; i < trimCount; i += 1) {
      ctx.seen.delete(entries[i][0]);
    }
  }

  function spawnPill(ctx, reaction, id) {
    if (!ctx.container) {
      return;
    }
    const pill = document.createElement("div");
    pill.className = "reaction-pill";
    pill.dataset.reactionId = id;

    const emojiEl = document.createElement("span");
    emojiEl.className = "reaction-pill__emoji";
    emojiEl.textContent = reaction.emoji || "✨";

    const contentEl = document.createElement("div");
    contentEl.className = "reaction-pill__content";

    const labelEl = document.createElement("p");
    labelEl.className = "reaction-pill__label";
    labelEl.textContent = reaction.label || reaction.title || "Fresh feedback";
    contentEl.appendChild(labelEl);

    const metaParts = [];
    const title = reaction.title && reaction.title !== reaction.label ? reaction.title : null;
    if (title) {
      metaParts.push(title);
    }
    const questionSet = reaction.question_set && reaction.question_set !== reaction.label ? reaction.question_set.replace(/_/g, " ") : null;
    if (questionSet && (!title || title.toLowerCase() !== questionSet.toLowerCase())) {
      metaParts.push(questionSet);
    }
    if (reaction.timestamp) {
      try {
        const timestamp = new Date(reaction.timestamp);
        if (!Number.isNaN(timestamp.getTime())) {
          const timeText = timestamp.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
          metaParts.push(timeText);
        }
      } catch (err) {
        // Ignore formatting errors
      }
    }
    if (metaParts.length) {
      const metaEl = document.createElement("p");
      metaEl.className = "reaction-pill__meta";
      metaEl.textContent = metaParts.join(" • ");
      contentEl.appendChild(metaEl);
    }

    pill.appendChild(emojiEl);
    pill.appendChild(contentEl);

    const drift = Math.round(Math.random() * 26 - 13);
    pill.style.setProperty("--fh-reaction-drift", `${drift}px`);
    const rawDuration = ctx.durationMs + Math.round(Math.random() * 900 - 450);
    const duration = Math.max(3200, rawDuration);
    pill.style.setProperty("--fh-reaction-duration", `${duration}ms`);

    pill.addEventListener(
      "animationend",
      () => {
        pill.remove();
      },
      { once: true }
    );

    while (ctx.container.childElementCount >= ctx.maxVisible) {
      const first = ctx.container.firstElementChild;
      if (!first) {
        break;
      }
      first.remove();
    }

    ctx.container.appendChild(pill);
  }

  function ingest(channel, reactions) {
    const ctx = registry.get(normalizeChannel(channel));
    if (!ctx || !Array.isArray(reactions) || reactions.length === 0) {
      return;
    }
    reactions.forEach((reaction) => {
      if (!reaction) {
        return;
      }
      const id = String(
        reaction.id || `${reaction.timestamp || ""}:${reaction.response_key || ""}:${reaction.value || Math.random().toString(36).slice(2)}`
      );
      if (ctx.seen.has(id)) {
        return;
      }
      ctx.seen.set(id, Date.now());
      pruneSeen(ctx);
      spawnPill(ctx, reaction, id);
    });
  }

  window.FlowHarmonyReactions = {
    mount,
    ingest,
  };
})();
