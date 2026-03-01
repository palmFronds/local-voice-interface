"""Cycle through all four orb states automatically for visual verification."""
import sys
import random
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QTimer
import ui

app = QApplication(sys.argv)
orb = ui.OrbWidget()
orb.show()

_states = ["inactive", "listening", "thinking", "speaking"]
_idx = [0]

def _cycle():
    ui.ui_state_queue.put(_states[_idx[0] % len(_states)])
    _idx[0] += 1

_cycle()  # Set inactive immediately on launch

_cycle_timer = QTimer()
_cycle_timer.setInterval(2500)
_cycle_timer.timeout.connect(_cycle)
_cycle_timer.start()

_rms_timer = QTimer()
_rms_timer.setInterval(100)
_rms_timer.timeout.connect(lambda: ui.ui_rms_queue.put(random.uniform(0.3, 0.8)))
_rms_timer.start()

sys.exit(app.exec())
