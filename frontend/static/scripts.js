function getDiscordInviteCode(input) {
  if (typeof input !== "string") return null;

  const m = input
    .trim()
    .match(
      /^https?:\/\/(?:www\.)?(?:discord\.gg|discord\.com\/invite)\/([A-Za-z0-9-]+)\/?$/,
    );

  return m ? m[1] : null;
}

function isDiscordSnowflake(value) {
  if (typeof value !== "string") return false;

  // must be digits only
  if (!/^\d+$/.test(value)) return false;

  // Discord snowflakes are typically 17–20 digits
  if (value.length < 16 || value.length > 20) return false;

  // ensure it fits into unsigned 64-bit
  const n = BigInt(value);
  const MAX = (1n << 64n) - 1n;

  return n >= 0n && n <= MAX;
}

function displayInvalidPreviewInputError(form, errorMessage) {
  let input = form.getElementsByTagName("input")[0].value;
  let errorMessageContainer = form.getElementsByTagName("p")[0];

  if (typeof errorMessage == "string") {
    errorMessageContainer.innerText = errorMessage;
  } else {
    errorMessageContainer.innerText = "";
  }
}

async function performPreviewRequest(parameter) {
  const url = "/preview/" + encodeURIComponent(parameter) + ".json";
  const data = await (await fetch(url)).json();
  return data;
}

function renderFeedPreview(guild_info) {
  /*
    Clear current preview, if any
    https://stackoverflow.com/a/3955238/5337349
  */
  const previewContainerElement = document.getElementById("previewContainer");
  previewContainerElement.textContent = "";

  const guildNameElement = document.createElement("h2");
  guildNameElement.innerText = guild_info.name;
  previewContainerElement.appendChild(guildNameElement);

  const feedURLContainer = document.createElement("input");
  feedURLContainer.readOnly = true;
  feedURLContainer.value = location.href + "feed/" + guild_info.id + ".ics";
  previewContainerElement.appendChild(feedURLContainer);
  feedURLContainer.select();

  /*
    A small gap between feed URL and "Copy" button
    to match Preview form style
  */
  previewContainerElement.appendChild(document.createTextNode(" "));

  const feedURLCopyButton = document.createElement("button");
  feedURLCopyButton.innerText = "Copy";
  feedURLCopyButton.onclick = () =>
    navigator.clipboard.writeText(feedURLContainer.value);
  previewContainerElement.appendChild(feedURLCopyButton);

  if (guild_info.events.length) {
    const eventListElement = document.createElement("ul");
    eventListElement.appendChild(
      ...guild_info.events.map((guild_event) => {
        const li = document.createElement("li");
        li.textContent = guild_event.name;
        return li;
      }),
    );
    previewContainerElement.appendChild(eventListElement);
  } else {
    const noEventsWarning = document.createElement("p");
    noEventsWarning.innerText =
      "No events found yet, but server access verified successfully!";
    previewContainerElement.appendChild(noEventsWarning);
  }
}

async function previewFormSubmitHandler(e) {
  e.preventDefault();

  const input = document.getElementById("previewInput");

  const endpointCallParameter = isDiscordSnowflake(input.value)
    ? input.value
    : getDiscordInviteCode(input.value);

  if (!endpointCallParameter) {
    displayInvalidPreviewInputError(
      e.srcElement,
      "Invalid input. " + input.placeholder,
    );
    return;
  }

  const previewData = await performPreviewRequest(endpointCallParameter);
  if (previewData.error) {
    displayInvalidPreviewInputError(e.srcElement, previewData.error);
    return;
  }

  displayInvalidPreviewInputError(e.srcElement);
  renderFeedPreview(previewData);
}

window.onload = function () {
  document
    .getElementById("previewForm")
    .addEventListener("submit", previewFormSubmitHandler);
};
