from stages.crews import run as run_crews
# ...
ctx.data["crews"] = run_crews(ctx)

from stages.crew_details import run as run_crew_details
# ...
ctx.data["crew_details"] = run_crew_details(ctx)

from stages.pirate_urls import run as run_pirate_urls
# ...
ctx.data["pirate_urls"] = run_pirate_urls(ctx)

from stages.pirates import run as run_pirates
# ...
ctx.data["pirates"] = run_pirates(ctx)

from stages.shoppes import run as run_shoppes
# ...
ctx.data["shoppes"] = run_shoppes(ctx)