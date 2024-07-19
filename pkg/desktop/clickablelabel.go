package desktop

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/widget"
)

type ClickableLabel struct {
	*widget.Label
	OnTapped      func()
	OnRightTapped func()
}

func NewClickableLabel(text string, onTapped func(), onRightTapped func()) *ClickableLabel {
	label := &ClickableLabel{
		Label:         widget.NewLabel(text),
		OnTapped:      onTapped,
		OnRightTapped: onRightTapped,
	}
	label.ExtendBaseWidget(label)
	return label
}

func (c *ClickableLabel) Tapped(ev *fyne.PointEvent) {
	if c.OnTapped != nil {
		c.OnTapped()
	}
}

func (c *ClickableLabel) TappedSecondary(ev *fyne.PointEvent) {
	if c.OnRightTapped != nil {
		c.OnRightTapped()
	}
}

func (c *ClickableLabel) CreateRenderer() fyne.WidgetRenderer {
	return c.Label.CreateRenderer()
}
