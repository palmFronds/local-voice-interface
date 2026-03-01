"""Orb UI for Simple Voice Interface.

Frameless 300x300 PyQt6 window containing an animated orb that reflects
the current pipeline state. External code pushes state strings into
ui_state_queue; audio energy floats go into ui_rms_queue.

No imports beyond PyQt6 and queue.
"""

import queue
import sys

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QGraphicsView, QGraphicsScene,
    QGraphicsDropShadowEffect,
)
from PyQt6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve, pyqtProperty
from PyQt6.QtGui import QColor, QBrush, QRadialGradient, QPen, QPainter

# ── Module-level queues ─────────────────────────────────────────────────────────
# Thread-safe: pipeline asyncio tasks can push from any thread.
ui_state_queue: queue.SimpleQueue = queue.SimpleQueue()
ui_rms_queue: queue.SimpleQueue = queue.SimpleQueue()

# ── Layout constants ────────────────────────────────────────────────────────────
_WINDOW_PX = 300
_DIAMETER = 120
_HALF = _DIAMETER / 2

# Per-state config: (core_hex, glow_hex, anim_duration_ms, scale_peak)
# duration=0 means no animation — orb sits completely still.
_STATE_CONFIG: dict[str, tuple[str, str, int, float]] = {
    "inactive":  ("#3a3a3a", "#2a2a2a", 0,    1.00),
    "listening": ("#00a8ff", "#0066cc", 2000, 1.08),
    "thinking":  ("#8b00ff", "#5500aa", 600,  1.12),
    "speaking":  ("#00ff88", "#00aa55", 1000, 1.15),
}


class OrbWidget(QMainWindow):
    """Frameless always-on-top window containing the animated orb.

    State changes arrive via ui_state_queue (polled every 50 ms).
    RMS energy arrives via ui_rms_queue and drives orb scale in speaking state.
    Right-click anywhere on the window quits the application.
    """

    def __init__(self) -> None:
        super().__init__()
        # Backing value for the custom QPropertyAnimation target.
        self._orb_scale_value: float = 1.0
        self._current_state: str = "inactive"

        self._setup_window()
        self._setup_scene()
        self._setup_animation()

        # Poll both queues every 50 ms — imperceptible lag, simpler than signals.
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(50)
        self._poll_timer.timeout.connect(self._poll_queues)
        self._poll_timer.start()

        # Boot in inactive state so the orb is immediately visible.
        self._apply_state("inactive")

    # ── Window ──────────────────────────────────────────────────────────────────

    def _setup_window(self) -> None:
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setFixedSize(_WINDOW_PX, _WINDOW_PX)
        self.setStyleSheet("QMainWindow { background-color: #0a0a0a; }")
        screen = QApplication.primaryScreen().availableGeometry()
        self.move(
            (screen.width() - _WINDOW_PX) // 2,
            (screen.height() - _WINDOW_PX) // 2,
        )

    # ── Scene ───────────────────────────────────────────────────────────────────

    def _setup_scene(self) -> None:
        self._scene = QGraphicsScene(self)
        # Scene origin at centre of window so the orb rect math stays simple.
        self._scene.setSceneRect(
            -_WINDOW_PX / 2, -_WINDOW_PX / 2, _WINDOW_PX, _WINDOW_PX
        )

        self._view = QGraphicsView(self._scene, self)
        self._view.setGeometry(0, 0, _WINDOW_PX, _WINDOW_PX)
        self._view.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._view.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # Antialiasing makes the gradient edge look smooth instead of pixelated.
        self._view.setRenderHint(QPainter.RenderHint.Antialiasing)
        self._view.setStyleSheet("background: transparent; border: none;")
        self._view.setBackgroundBrush(QBrush(QColor("#0a0a0a")))
        self.setCentralWidget(self._view)

        # Orb — centered at scene origin; rect updated by orb_scale setter.
        self._orb = self._scene.addEllipse(
            -_HALF, -_HALF, _DIAMETER, _DIAMETER,
            QPen(Qt.PenStyle.NoPen),
            QBrush(QColor("#3a3a3a")),
        )

        # Drop-shadow provides the outer glow; offset=0 means symmetric halo.
        self._glow = QGraphicsDropShadowEffect()
        self._glow.setOffset(0, 0)
        self._glow.setBlurRadius(25)
        self._glow.setColor(QColor("#2a2a2a"))
        self._orb.setGraphicsEffect(self._glow)

    # ── Animation ───────────────────────────────────────────────────────────────

    def _setup_animation(self) -> None:
        # QPropertyAnimation drives the custom orb_scale property so Qt handles
        # interpolation and easing — no manual timer math needed.
        self._anim = QPropertyAnimation(self, b"orb_scale")
        self._anim.setLoopCount(-1)  # Repeat until .stop() is called on transition.

    # ── Queue polling ────────────────────────────────────────────────────────────

    def _poll_queues(self) -> None:
        # Drain state queue; only the latest value matters (skip stale entries).
        new_state: str | None = None
        while not ui_state_queue.empty():
            try:
                val = ui_state_queue.get_nowait()
                if isinstance(val, str) and val in _STATE_CONFIG:
                    new_state = val
            except Exception:
                break
        if new_state is not None and new_state != self._current_state:
            self._apply_state(new_state)

        # In speaking state drive scale directly from the latest RMS value;
        # the idle animation provides a fallback when the queue is empty.
        if self._current_state == "speaking":
            rms: float | None = None
            while not ui_rms_queue.empty():
                try:
                    rms = ui_rms_queue.get_nowait()
                except Exception:
                    break
            if isinstance(rms, (int, float)):
                # Clamp to [0, 1] defensively; scale range 1.0–1.2.
                self.orb_scale = 1.0 + min(max(float(rms), 0.0), 1.0) * 0.2

    # ── State transitions ────────────────────────────────────────────────────────

    def _apply_state(self, state: str) -> None:
        """Swap orb color, glow, and animation for the new state."""
        self._current_state = state
        color_hex, glow_hex, duration, scale_peak = _STATE_CONFIG[state]

        self._update_brush(color_hex)
        self._glow.setColor(QColor(glow_hex))

        self._anim.stop()
        if duration > 0:
            self._anim.setDuration(duration)
            # Three key frames produce a symmetric breathe: 1.0 → peak → 1.0.
            # Each loop repeats the full oscillation, so there is no stutter at
            # the loop boundary (start and end values are identical).
            self._anim.setKeyValueAt(0.0, 1.0)
            self._anim.setKeyValueAt(0.5, float(scale_peak))
            self._anim.setKeyValueAt(1.0, 1.0)
            self._anim.setEasingCurve(QEasingCurve.Type.InOutSine)
            self._anim.start()
        else:
            # Inactive: snap back to resting size with no animation.
            self.orb_scale = 1.0

    def _update_brush(self, color_hex: str) -> None:
        """Apply a radial gradient to give the orb a 3D sphere appearance.

        Light source is simulated at the top-left (offset -0.3 × radius),
        so the highlight sits in the upper-left quadrant and the shadow
        falls to the lower-right — the classic CG sphere look.
        """
        color = QColor(color_hex)
        # Focal point offset from centre to fake a top-left light source.
        gradient = QRadialGradient(-_HALF * 0.3, -_HALF * 0.3, _DIAMETER)
        gradient.setColorAt(0.0, color.lighter(160))  # Bright specular highlight
        gradient.setColorAt(0.55, color)               # Natural mid-tone
        gradient.setColorAt(1.0, color.darker(230))    # Deep shadow at limb
        self._orb.setBrush(QBrush(gradient))

    # ── Custom property ──────────────────────────────────────────────────────────

    @pyqtProperty(float)
    def orb_scale(self) -> float:
        """Scale factor applied uniformly to the orb diameter."""
        return self._orb_scale_value

    @orb_scale.setter
    def orb_scale(self, value: float) -> None:
        self._orb_scale_value = value
        d = _DIAMETER * value
        r = d / 2
        # Recentre the rect so the orb expands symmetrically around scene origin.
        self._orb.setRect(-r, -r, d, d)

    # ── Input ────────────────────────────────────────────────────────────────────

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.RightButton:
            QApplication.quit()
        super().mousePressEvent(event)


def main() -> None:
    """Standalone entry point for visual testing without the voice pipeline."""
    app = QApplication(sys.argv)
    orb = OrbWidget()
    orb.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
