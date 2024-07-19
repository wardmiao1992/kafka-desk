package desktop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/data/binding"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	localKafka "kafka-desk/pkg/kafka"
	"log"
	"slices"
	"time"
)

var a fyne.App
var mainWindow fyne.Window
var newConnWindow fyne.Window
var mainSplitContainer *container.Split
var messageContainer *fyne.Container

var activeKafkaClient *localKafka.Client
var activeTopicsBinder = binding.BindStringList(&[]string{})
var activeTopic string
var activeTopicBinder = binding.BindString(&activeTopic)
var newConnWindowOpened = false

type Connection struct {
	Name string
	Host string
	Port string
}

func StartDesktop() {
	a = app.New()
	mainWindow = a.NewWindow("Kafka Desktop")
	mainWindow.SetMaster()

	messageContainer = container.NewStack()

	// blank home container
	blankHomeContainer := container.NewCenter(widget.NewButtonWithIcon("New Connection", theme.ContentAddIcon(), func() {
		NewConnectionFunc()
	}))
	blankHome := container.NewStack(blankHomeContainer)
	mainWindow.SetContent(blankHome)

	// menu
	connMenu := fyne.NewMenu("Connection",
		fyne.NewMenuItem("New Connection", func() {
			NewConnectionFunc()
		}),
	)
	// Create the main menu
	mainMenu := fyne.NewMainMenu(
		connMenu,
	)
	mainWindow.SetMainMenu(mainMenu)

	//------------------ message container -----------------------

	// Create message binding and list container
	messagesBinder := binding.BindStringList(&[]string{})
	messageList := widget.NewListWithData(
		messagesBinder,
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(item binding.DataItem, object fyne.CanvasObject) {
			object.(*widget.Label).Bind(item.(binding.String))
		},
	)
	messageList.OnSelected = func(id widget.ListItemID) {
		messages, err := messagesBinder.Get()
		if err != nil {
			fmt.Println("err: ", err)
			return
		}

		// Create a buffer to hold the pretty JSON
		var prettyMsg bytes.Buffer
		// Indent the JSON
		err = json.Indent(&prettyMsg, []byte(messages[id]), "", "    ")
		if err != nil {
			dialog.ShowInformation("Error", "failed to beautify json", mainWindow)
			return
		}

		messageDetailsEntry := widget.NewMultiLineEntry()
		messageDetailsEntry.SetText(prettyMsg.String())
		messageDetailsEntry.SetMinRowsVisible(20)
		var messageDetailPopup *widget.PopUp
		messageDetailPopup = widget.NewModalPopUp(container.NewStack(container.NewVBox(messageDetailsEntry, container.NewCenter(widget.NewButton("OK", func() {
			messageDetailPopup.Hide()
		})))), mainWindow.Canvas())
		messageDetailPopup.Resize(fyne.NewSize(500, 400))
		messageDetailPopup.Show()
	}
	messageContainer.Add(messageList)

	// ----------------- topic tip and toolbar---------------------
	topicTipLabel := widget.NewLabel("")
	topicTipLabel.Bind(activeTopicBinder)
	topicTip := container.NewHBox(widget.NewLabel("Topic: "), topicTipLabel)
	// create toolbar
	toolbar := widget.NewToolbar(
		widget.NewToolbarSpacer(),
		widget.NewToolbarAction(theme.ContentAddIcon(), func() {
			// create new single message
			var createMessagePopup *widget.PopUp
			messageBinder := binding.BindString(nil)
			messageEntry := widget.NewMultiLineEntry()
			messageEntry.SetMinRowsVisible(10)
			messageEntry.Bind(messageBinder)
			newMessageContainer := container.NewBorder(container.NewCenter(widget.NewLabel("New Message")),
				container.NewHBox(layout.NewSpacer(), widget.NewButton("Cancel", func() {
					createMessagePopup.Hide()
				}), widget.NewButton("Publish", func() {
					createMessagePopup.Hide()
					message, err := messageBinder.Get()
					if err != nil {
						log.Println(err)
						return
					}
					topic, err := activeTopicBinder.Get()
					if err != nil {
						log.Println(err)
						return
					}
					loadingPopup := widget.NewModalPopUp(container.NewStack(widget.NewProgressBarInfinite()), mainWindow.Canvas())
					loadingPopup.Show()
					if err = createMessage(topic, message); err != nil {
						log.Println(err)
						loadingPopup.Hide()
						return
					}
					loadingPopup.Hide()
					dialog.ShowInformation("New Message", "message published", mainWindow)
				})), nil, nil, container.NewStack(messageEntry))
			createMessagePopup = widget.NewModalPopUp(container.NewStack(newMessageContainer), mainWindow.Canvas())
			createMessagePopup.Resize(fyne.NewSize(400, 400))
			createMessagePopup.Show()
		}),
		widget.NewToolbarAction(theme.DeleteIcon(), func() {
			topic, err := activeTopicBinder.Get()
			if err != nil {
				dialog.ShowInformation("Error", "failed to get selected topic: "+err.Error(), mainWindow)
				return
			}
			dialog.ShowConfirm("Delete topic", "Are you sure to delete topic ["+topic+"]", func(b bool) {
				if b {
					loadingPopup := widget.NewModalPopUp(widget.NewProgressBarInfinite(), mainWindow.Canvas())
					loadingPopup.Show()
					if err = deleteTopic(topic); err != nil {
						loadingPopup.Hide()
						dialog.ShowInformation("Error", "failed to delete selected topic ["+topic+"]: "+err.Error(), mainWindow)
						return
					}
					dialog.ShowInformation("Delete topic ["+topic+"]", "success", mainWindow)
					loadingPopup.Hide()
				}
			}, mainWindow)
		}),
	)
	toolbarContainer := container.NewHBox(topicTip, layout.NewSpacer(), toolbar)
	// combine message container and toolbar into container
	messageContent := container.NewBorder(toolbarContainer, nil, nil, nil, messageContainer)

	// create topics list container
	topicList := widget.NewListWithData(
		activeTopicsBinder,
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(item binding.DataItem, object fyne.CanvasObject) {
			topicLabel := object.(*widget.Label)
			topicBinding := item.(binding.String)
			topicLabel.Bind(topicBinding)
		},
	)
	topicList.OnSelected = func(id widget.ListItemID) {
		topic, err := activeTopicsBinder.GetValue(id)
		activeTopicBinder.Set(topic)
		if err != nil {
			fmt.Println("failed to get topic: ", err)
			return
		}
		fmt.Println("select topic: " + topic)
		loadingPopup := widget.NewModalPopUp(widget.NewProgressBarInfinite(), mainWindow.Canvas())
		loadingPopup.Show()
		messages, err := activeKafkaClient.SearchMessage(topic, "", time.Now().UTC(), time.Now().UTC().Add(-1*time.Hour))
		loadingPopup.Hide()
		if err != nil {
			dialog.ShowInformation("Error", "failed to load messages: "+err.Error(), mainWindow)
			return
		}
		messagesBinder.Set(messages)
		if len(messages) == 0 {
			dialog.ShowInformation("Search Messages", "No Message Found", mainWindow)
			return
		}
		messageContent.Show()
	}
	topicList.OnUnselected = func(id widget.ListItemID) {
		activeTopicBinder.Set("")
		messagesBinder.Set([]string{})
	}

	// Split the window into sidebar and content area
	mainSplitContainer = container.NewHSplit(topicList, messageContent)
	mainSplitContainer.SetOffset(0.3) // Set initial size of the sidebar

	mainWindow.Resize(fyne.NewSize(800, 600))

	mainWindow.ShowAndRun()

}

func OpenConnection(conn *Connection) (*localKafka.Client, error) {
	kafkaConfig := &localKafka.Config{Host: conn.Host, Port: conn.Port}
	kafkaClient, err := localKafka.NewClient(kafkaConfig)
	if err != nil {
		panic(err)
	}
	return kafkaClient, nil
}

func NewConnectionFunc() {
	if newConnWindowOpened {
		fmt.Println("connection windows already opened")
		return
	}
	newConnWindow = a.NewWindow("Create New Connection")

	hostLabel := widget.NewLabel("Host:")
	hostInput := widget.NewEntry()
	hostInput.SetPlaceHolder("Host")
	portLabel := widget.NewLabel("Port:")
	portInput := widget.NewEntry()
	portInput.SetPlaceHolder("Port")
	openNewConnButton := widget.NewButton("Open", func() {
		newConnWindow.Close()
		loadingPopup := widget.NewModalPopUp(widget.NewProgressBarInfinite(), mainWindow.Canvas())
		loadingPopup.Show()
		newConn := &Connection{}
		if hostInput.Text == "" {
			newConn.Host = "localhost"
		} else {
			newConn.Host = hostInput.Text
		}
		if portInput.Text == "" {
			newConn.Port = "9092"
		} else {
			newConn.Port = portInput.Text
		}
		var err error
		activeKafkaClient, err = OpenConnection(newConn)
		if err != nil {
			dialog.ShowInformation("Error", "failed to open connection: "+err.Error(), mainWindow)
			return
		}
		topicsChain := make(chan []string)
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		go loadTopics(topicsChain)
		select {
		case topics := <-topicsChain:
			activeTopicsBinder.Set(topics)
			loadingPopup.Hide()
			mainWindow.SetContent(mainSplitContainer)
			close(topicsChain)
		case <-ctx.Done():
			dialog.ShowInformation("Error", "failed to get topics: timeout", mainWindow)
			close(topicsChain)
		}
	})
	newConnContainer := container.NewVBox(hostLabel, hostInput, portLabel, portInput, openNewConnButton)
	newConnWindow.SetContent(newConnContainer)
	newConnWindow.Resize(fyne.NewSize(400, 150))
	newConnWindowOpened = true
	newConnWindow.SetOnClosed(func() {
		newConnWindowOpened = false
	})
	newConnWindow.Show()
}

func loadTopics(ch chan<- []string) {
	topics, err := activeKafkaClient.ListTopics()
	if err != nil {
		panic(err)
	}
	slices.Sort(topics)
	ch <- topics
}

func createMessage(topic, message string) error {
	// Create a buffer to hold the compact JSON
	var compactMsg bytes.Buffer

	// Compact the JSON
	err := json.Compact(&compactMsg, []byte(message))
	if err != nil {
		return err
	}
	return activeKafkaClient.WriteMessage(topic, compactMsg.String())
}

func deleteTopic(topic string) error {
	return activeKafkaClient.DeleteTopic(topic)
}
