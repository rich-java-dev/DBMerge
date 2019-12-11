package view;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Client extends Application {

  @Override
  public void start(Stage stage) throws Exception {
   Scene scene = new Scene(new MainView(), 1000, 750);
   
   scene.getStylesheets().addAll("");
   
   stage.setScene(scene);
   stage.show();
   
  }

  public static void main(String[] args) {
    launch(args);
  }

}
