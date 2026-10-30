-- Pandoc filter for the Pages build. Relative links are written for GitHub's
-- file view; on the rendered site, Markdown pages under docs/ are .html files
-- (README.md becomes index.html), and anything else lives only in the repo,
-- so those links point at GitHub.
local repo = "https://github.com/" .. (os.getenv("GITHUB_REPOSITORY") or "") .. "/blob/main/"
local dir = PANDOC_STATE.input_files[1]:match("^(.*)/") or "."

local function exists(path)
  local f = io.open(path, "r")
  if f then f:close() end
  return f ~= nil
end

local function in_site(path)
  if not path:match("^docs/") then return false end
  if path:match("/$") then return exists(path .. "README.md") end
  return true
end

function Link(el)
  local t = el.target
  if t:match("^%a[%w+.-]*:") or t:match("^#") then return nil end
  local path = (dir == "." and "" or dir .. "/") .. t
  if not in_site(path) then
    el.target = repo .. path
    return el
  end
  if dir == "." then t = t:gsub("^docs/", "") end
  t = t:gsub("README%.md", "index.html"):gsub("/$", "/index.html"):gsub("%.md(#?)", ".html%1")
  el.target = t
  return el
end

function Image(el)
  if dir == "." then el.src = el.src:gsub("^docs/", "") end
  return el
end
