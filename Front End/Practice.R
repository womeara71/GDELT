library(shiny)


ui <- fluidPage(
  titlePanel(title = "Event Prediction Results"),
  
  mainPanel(
    plotOutput("results")
  )
  
)

server <- function(input, output, session){
  data <- c(12,25)
  
  output$results <- renderPlot({
    
    barplot(data)
    
  })
  
}

shinyApp(ui, server)