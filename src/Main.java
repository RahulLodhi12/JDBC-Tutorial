import java.sql.*;
import java.util.Scanner;

public class Main {
//    private static final String url="jdbc:mysql://localhost:3306/mydb";
    private static final String url="jdbc:mysql://localhost:3306/{your_database_name}";
    private static final String username="root";
    private static final String password="{your_password}";

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e){
            e.printStackTrace();
        }

        try{
            Connection connection = DriverManager.getConnection(url,username,password);

            /*CRUD Operations Using Statement:*/

            /* SELECT * FROM TABLE_NAME */
            Statement statement = connection.createStatement();

            String query = "select * from students";
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()){ //iterate all rows
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                double marks = resultSet.getDouble("marks");

                System.out.println(id + " " + name + " " + age + " " + marks);
            }


            /* INSERT INTO TABLE_NAME */
            String query = String.format("insert into students(name,age,marks) values('%s',%d,%f)","Rahul",27,98.7);
            int rowsAffected = statement.executeUpdate(query);
            if(rowsAffected>0){
                System.out.println("Data Inserted Successfully");
            }
            else {
                System.out.println("Data Not Inserted");
            }

            /*UPDATE TABLE_NAME SET age=23 where id=2*/
            String query = String.format("update students set age=%d, marks=%f where id=%d", 23,98.7,2);
            int rowsAffected = statement.executeUpdate(query);
            if(rowsAffected>0){
                System.out.println("Data Updated Successfully");
            }
            else{
                System.out.println("Data Not Updated!!");
            }

            /*DELETE FROM TABLE_NAME where id=1*/
            String query = "delete from students where id=1";
            int rowsAffected = statement.executeUpdate(query);
            if(rowsAffected>0){
                System.out.println("Deleted Successfully");
            }
            else{
                System.out.println("Not Deleted!!");
            }


            /*----------------------X-----------------------------X------------------------------X--------------------------X--------------------*/



            /*CRUD Operations Using PreparedStatement:*/

            /*INSERT INTO TABLE_NAME*/
            String query = "insert into students(name,age,marks) values(?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(query); //query is compiled by this line -> once and last time.

            preparedStatement.setString(1,"Ankita");
            preparedStatement.setInt(2,25);
            preparedStatement.setDouble(3,79.9);

            int rowsAffected = preparedStatement.executeUpdate();
            if(rowsAffected>0){
                System.out.println("Data Inserted Successfully");
            }
            else{
                System.out.println("Data Not Inserted!!");
            }

            /*SELECT MARKS FROM TABLE_NAME where name="Rahul"*/
            String query = "select marks from students where name= ?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,"Rahul");

            ResultSet resultSet = preparedStatement.executeQuery(); //ResultSet is used when we retrieve data from table
            if(resultSet.next()){
                double marks = resultSet.getDouble("marks");
                System.out.println("Marks: "+ marks);
            }
            else{
                System.out.println("Marks not found!!");
            }

            /*UPDATE TABLE_NAME set marks=77.7 where id=3*/
            String query = "update students set marks=? where id=?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setDouble(1,77.7);
            preparedStatement.setInt(2,3);

            int rowAffected = preparedStatement.executeUpdate();
            if(rowAffected>0){
                System.out.println("Data Updated Successfully");
            }
            else{
                System.out.println("Data Not Updated!!");
            }


            /*DELETE FROM TABLE_NAME where id=3*/
            String query = "delete from students where id=?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1,3);

            int rowAffected = preparedStatement.executeUpdate();
            if(rowAffected>0){
                System.out.println("Data Deleted Successfully");
            }
            else{
                System.out.println("Data Not Deleted!!");
            }



            /*----------------------X-----------------------------X------------------------------X--------------------------X--------------------*/



            /*Batch Processing in JDBC: By Statement*/
            Statement statement = connection.createStatement();

            Scanner sc = new Scanner(System.in);
            while(true){
                System.out.println("Enter name: ");
                String name = sc.nextLine();
                System.out.println("Enter age: ");
                int age = sc.nextInt();
                System.out.println("Enter marks: ");
                double marks = sc.nextDouble();
                System.out.println("Want to Exit(1/0) ?");
                int e = sc.nextInt(); sc.nextLine();

                String query = String.format("insert into students(name,age,marks) values('%s',%d,%f)",name,age,marks);
                statement.addBatch(query);

                if(e==1) break;
            }

            int[] arr = statement.executeBatch();
            for(int i=0;i<arr.length;i++){
                if(arr[i]==0) {
                    System.out.println("Query: " + i + "Not Executed successfully");
                }
            }



            /*----------------------X-----------------------------X------------------------------X--------------------------X--------------------*/



            /*Batch Processing in JDBC: By PreparedStatement*/
            String query = "insert into students(name,age,marks) values(?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            Scanner sc = new Scanner(System.in);
            while(true){
                System.out.println("Enter name: ");
                String name = sc.nextLine();
                System.out.println("Enter age: ");
                int age = sc.nextInt();
                System.out.println("Enter marks: ");
                double marks = sc.nextDouble();
                System.out.println("Want to Exit(1/0) ?");
                int e = sc.nextInt(); sc.nextLine();

                preparedStatement.setString(1,name);
                preparedStatement.setInt(2,age);
                preparedStatement.setDouble(3,marks);

                preparedStatement.addBatch();

                if(e==1) break;
            }

            int[] arr = preparedStatement.executeBatch();
            for(int i=0;i<arr.length;i++){
                if(arr[i]==0) {
                    System.out.println("Query: " + i + "Not Executed successfully");
                }
            }



            /*----------------------X-----------------------------X------------------------------X--------------------------X--------------------*/



            /*Transaction Handling in JDBC*/

            connection.setAutoCommit(false);

            String debit_query = "update accounts set balance = balance - ? where account_number = ?";
            String credit_query = "update accounts set balance = balance + ?  where account_number = ?";

            PreparedStatement debitPreparedStatement = connection.prepareStatement(debit_query);
            PreparedStatement creditPreparedStatement = connection.prepareStatement(credit_query);

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter Debit Amount:");
            double debit_amount = sc.nextDouble();

            debitPreparedStatement.setDouble(1,debit_amount);
            debitPreparedStatement.setInt(2,101);

            creditPreparedStatement.setDouble(1,debit_amount);
            creditPreparedStatement.setInt(2,102);

            debitPreparedStatement.executeUpdate();
            creditPreparedStatement.executeUpdate();

            if(isSufficient(connection,101,debit_amount)){
                connection.commit();
                System.out.println("Transaction Successfully and Committed!!");
            }
            else{
                connection.rollback();
                System.out.println("Transaction Failed and Rollback!!");
            }

            debitPreparedStatement.close();
            creditPreparedStatement.close();
            sc.close();
            connection.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static boolean isSufficient(Connection connection, int account_number, double amount){
        try{
            String query = "select balance from accounts where account_number=?";
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1,account_number);

            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                double current_balance = resultSet.getDouble("balance");
                if(amount>current_balance){
                    return false;
                }
                else{
                    return true;
                }
            }
            resultSet.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}