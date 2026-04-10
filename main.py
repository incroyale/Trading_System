# main.py
from analytics.graphs import vol_cone, iv_vs_rv

# ---------------  Pre-market  --------------
iv_vs_rv()
vol_cone()

# # --------------- Start dashboard (everything below runs after market close) --------------
from dashboard.app import app
app.run(debug=False)


# ------------------ Post-market --------------------------