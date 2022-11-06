package enrique.act3_4;

import java.sql.*;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Act3_4 {

    public static void main(String[] args) {
        menu();                              // CREA EL MENU Y TE DA A ELEGIR LOS METODOS O SALIR DEL PROGRAMA
        //newDepartmentProcedure();          // CREA EL PROCEDIMIENTO PARA CREAR NUEVOS DEPARTAMENTOS
        //newTeacherProcedure();             // CREA EL PROCEDIMIENTO PARA CREAR NUEVOS PROFESORES
        //newDepartment();                   // CREA UN NUEVO DEPARTAMENTO
        // newTeacher();                     // CREA UN NUEVO PROFESOR
        //set_Salary();                      // LE PONE UN SALARIO A TODOS LOS PROFESORES
        //salary_Porcentaje_Procedure();     // CREA EL PROCEDIMIENTO PARA EL AUMENTO EN PORCENJATE DE LOS PROFESORES
        //salary_Porcentaje();               // AÑADE UN AUMENTO DE PORCENTAJE AL SUELDO DE LOS PROFESORES
        //salary_Porcentaje_Department_Procedure();  // CREA EL PROCEDIMIENTO PARA SUBIR POR EL SUELDO AL PROFESOR POR DEPARTAMENTO
        //salary_Porcentaje_Department();            // AUMENTA EL SALARIO DEL PROFESOR POR DEPARTAMENTO
        // get_Newest_Teacher();             // DEVUELVE EL ULTIMO PROFESOR POR ID (PK)
       // get_Newest_Teacher2_Procedure();   // PROCEDIMIENTO SELECCIONA EL ULTIMO PROFESOR POR ID (OUT) 
        //get_Newest_Teacher2();            // DEVUELVE EL NOMBRE DEL ULTIMO PROFESOR POR ID REGISTRADO EN LA BD
      // count_Teacher_By_Department_Procedure();   // PROCEDIMIENTO (IN & OUT) PARA BUSCAR PROFESORES POR DEPARTAMENTO
        //count_Teacher_By_Department();             // BUSCA PROFESORES POR DEPARTAMENTO (PERO ME SACA EL TOTAL DE TODOS LOS PROFESORES NO FILTRA DEPARTAMENTO)
        //exit();                            // TE PREGUNTA SI QUIERES SALIR DEL PROGRAMA
       // consultaDB();                      // HACE UNA CONSULTA A LA BASE DE DATOS

    }

    public static Connection conexion(String path) {
        try {
            Connection con;
            return con = DriverManager.getConnection("jdbc:hsqldb:.\\database\\db");
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    } // CREA CONEXION CON HSQLDB

    public static void showResultSet(ResultSet res) {
        try {
            ResultSetMetaData rsmd = res.getMetaData();
            //get column num
            int colNum = rsmd.getColumnCount();
            System.out.println("-----------------------------");
            String aux = "";
            while (res.next()) {
                for (int i = 1; i <= colNum; i++) {
                    aux += (rsmd.getColumnName(i) + " : " + res.getString(i) + "; ");
                }
                aux += "\n";
            }
            System.out.println(aux);
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // MUESTRA EL RESULTADO DE UN RESULTSET PASADO POR PARAMETRO

    public static void consultaDB() {
        try {
            String query = "";
            String opcion = "";
            do {

                Scanner tcl = new Scanner(System.in);
                Connection con = conexion("jdbc:hsqldb:.\\database\\db");
                Statement stt = con.createStatement();
                System.out.println("Dime una sentencia sql(SIN ; final)");
                query = tcl.nextLine();
                ResultSet rs = stt.executeQuery(query); // guarda el resultado de la query en el resulset
                showResultSet(rs);
                
                rs.close();
                con.close();

                System.out.println("SI QUIERES OTRA SENTENCIA DALE ENTER, SI QUIERES SALIR ESCRIBE SALIR");
                Scanner sc = new Scanner(System.in);
                opcion = sc.nextLine();

            } while (!opcion.equalsIgnoreCase("salir"));

            System.out.println("\n SALIENDO DEL PROGRAMA!!! ");

        } catch (SQLException ex) {
            System.out.println("QUERY SQL INCORRECTA!!!");
        }
    } // HACE CONSULTAS A LA DB HSQLDB HASTA QUE LE DICES SALIR

    public static void newDepartment() {
        Scanner tcl = new Scanner(System.in); // Scanner para strings
        Scanner tclInt = new Scanner(System.in); // Scanner para int (tema buffer no hay lios)
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            
            CallableStatement call = con.prepareCall(" call set_new_department(?,?,?)");
            
            int dept;
            String name, office;
            
            System.out.println("Dime el numero de departamento");
            dept = tclInt.nextInt();
            call.setInt(1, dept); // posicion 1, valor x
            
            System.out.println("Dime el nombre del departamento nuevo");
            name = tcl.nextLine();
            call.setString(2, name);// posicion 2, valor x
            
            System.out.println("Dime el nombre de la oficina");
            office = tcl.nextLine();
            call.setString(3, office); // posicion 3, valor x
            
            call.execute();// ejecuta todo el call
            con.close();

        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // AÑADE UN NUEVO REGISTRO (PEDIDO AL USUARIO) A LA TABLA DEPARTMENTS (CallableStatement)

    public static void newDepartmentProcedure() {
        try {

            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();

            String sql = "DROP PROCEDURE set_new_department IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE set_new_department(dept_num INT, name varchar(20), office varchar(20))"
                    + "modifies sql data "
                    + "BEGIN ATOMIC "
                    + "INSERT INTO departments values(dept_num, name, office); "
                    + "END";

            stt.executeUpdate(sql);
            con.close();

        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // CREA PROCEDIMIENTO PARA AÑADIR NUEVOS REGISTROS A LA TABLA DEPARTMENTS (HSQLDB)(INSERT INTO VALUES)

    public static void newTeacher() {

        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            
            int id, dept, salary;
            String name, surname, email, date;
            
            Scanner tcl = new Scanner(System.in);
            Scanner tclInt = new Scanner(System.in);
            
            CallableStatement call = con.prepareCall(" call set_new_teacher(?,?,?,?,?,?,?)"); // llamada a procedure
            
            System.out.println("Dime el numero de ID para el profesor");
            id = tclInt.nextInt();
            call.setInt(1, id); // posicion 1, valor x
            
            System.out.println("Dime el nombre del profesor");
            name = tcl.nextLine();
            call.setString(2, name);// posicion 2, valor x
            
            System.out.println("Dime el apellido del profesor");
            surname = tcl.nextLine();
            call.setString(3, surname);
            
            System.out.println("Dime el correo del profesor");
            email = tcl.nextLine();
            call.setString(4, email);
            
            System.out.println("Dime la fecha del profesor(YYYY-MM-DD)");
            date = tcl.nextLine();
            call.setDate(5, Date.valueOf(date));
            
            System.out.println("Dime el departamento del profesor(10 o 40)");
            dept = tclInt.nextInt();
            call.setInt(6, dept);
            
            System.out.println("Dime el salario del profesor");
            salary = tclInt.nextInt();
            call.setInt(7, salary);
            
            call.execute();
            con.close();
            
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // AÑADE UN NUEVO REGISTRO (PEDIDO AL USUARIO) A LA TABLA TEACHERS (CallableStatement)

    public static void newTeacherProcedure() {
        try {

            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();

            String sql = "DROP PROCEDURE set_new_teacher IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE set_new_teacher(id INT, name varchar(20), surname varchar(20), email varchar(50), start_date DATE, dept_num INT, salary INT)"
                    + "modifies sql data "
                    + "BEGIN ATOMIC "
                    + "INSERT INTO teachers values(id, name, surname, email, start_date, dept_num, salary); "
                    + "END";

            stt.executeUpdate(sql);
            con.close();

        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // CREA PROCEDIMIENTO PARA AÑADIR NUEVOS REGISTROS A LA TABLA TEACHERS (HSQLDB)(INSERT INTO VALUES)

    public static void set_SalaryProcedure() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();

            String sql = "DROP PROCEDURE set_Salary IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE set_Salary (salario INT) "
                    + "modifies sql data "
                    + "BEGIN ATOMIC "
                    + "UPDATE teachers SET salary = salario;"
                    + "END";
            stt.executeUpdate(sql);
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // CREA PROCEDIMIENTO PARA CAMBIAR A TODOS LOS PROFESORES AL MISMO SALARIO

    public static void set_Salary() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Scanner tcl = new Scanner(System.in);
            int salario;
            
            CallableStatement call = con.prepareCall(" call set_Salary(?)");
            System.out.println("Dime el salario para los profesores");
            salario = tcl.nextInt();
            call.setInt(1, salario);
            
            call.execute();
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // MODIFICA EL SALARIO DE TODOS LOS PROFESORES POR IGUAL (PEDIDO AL USUARIO) (CallableStatement)

    public static void salary_Porcentaje_Procedure() {

        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();

            String sql = "DROP PROCEDURE salary_Porcentaje_Procedure IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE salary_Porcentaje_Procedure(p FLOAT)"
                    + "modifies sql data "
                    + "BEGIN ATOMIC "
                    + "UPDATE teachers SET Salary=Salary+((p*Salary)/100); "
                    + "END";

            stt.executeUpdate(sql);
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // CREA PROCEDIMIENTO PARA AUMENTAR A TODOS LOS PROFESORES EL SALARIO POR PORCENTAJE

    public static void salary_Porcentaje() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Scanner tcl = new Scanner(System.in);
            int salario;
            CallableStatement call = con.prepareCall(" call salary_Porcentaje_Procedure(?)");
            System.out.println("Dime el porcenjate del  salario para aumentar a los profesores (SIN %)");
            salario = tcl.nextInt();
            call.setInt(1, salario);
            call.execute();
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // MODIFICA EL SALARIO DE TODOS LOS PROFESORES POR % AUMENTANDOSELO (CallableStatement)

    public static void salary_Porcentaje_Department_Procedure() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();

            String sql = "DROP PROCEDURE salary_Porcentaje_Department_Procedure IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE salary_Porcentaje_Department_Procedure(p FLOAT, nombre Varchar(20))"
                    + "modifies sql data "
                    + "BEGIN ATOMIC "
                    + "UPDATE teachers SET salary = salary+((p*salary)/100) WHERE dept_num =(SELECT dept_num from departments WHERE name=nombre);"
                    + "END";

            stt.executeUpdate(sql);
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // CREA PROCEDIMIENTO PARA AUMENTAR EL SALARIO POR % DE LOS PROFESORES POR DEPARTAMENTO (2 TABLAS)

    public static void salary_Porcentaje_Department() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Scanner tcl = new Scanner(System.in);
            Scanner sc = new Scanner(System.in);
            float salario;
            String name;
            CallableStatement call = con.prepareCall(" call salary_Porcentaje_Department_Procedure(?,?)");
            System.out.println("Dime el porcenjate del salario para aumentar a un departamento (SIN %)");
            salario = tcl.nextInt();
            System.out.println("Dime el nombre del departamento que quieres aumentar");
            name = sc.nextLine();
            call.setFloat(1, salario);
            call.setString(2, name);
            call.execute();
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // MODIFICA EL SALARIO DE LOS PROFESORES DE X DEPARTAMENTO POR % (CallableStatement)

    public static void get_Newest_Teacher() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();
            String query = "Select TOP 1 name from teachers ORDER BY id DESC";
            ResultSet rs = stt.executeQuery(query);
            showResultSet(rs);
            rs.close();
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // MUESTRA EL NOMBRE DEL ULTIMO PROFESOR (EN ESTE CASO EL ULTIMO ID)

    public static void get_Newest_Teacher2_Procedure() {
       
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();
            String sql = "DROP PROCEDURE get_Newest_Teacher2_Procedure IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE get_Newest_Teacher2_Procedure(OUT id Varchar(10))"
                    + "READS sql DATA "
                    + "BEGIN ATOMIC "
                    + "SET id = (Select TOP 1 name from teachers ORDER BY id DESC); " // pasar al set el parametro out
                    + "END";
            stt.executeUpdate(sql);
            con.close();

        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // PROCEDIMIENTO SELECCIONA EL ULTIMO PROFESOR POR ID (OUT)

    public static void get_Newest_Teacher2() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();
            CallableStatement call = con.prepareCall(" call get_Newest_Teacher2_Procedure(?)");
            
            call.registerOutParameter(1, Types.VARCHAR); // pasar el primer parametro out y definir su tipo
            call.execute();
            System.out.println("El ultimo profesor es: "+call.getString(1)); // muestra por pantalla
            con.close();
        } catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // DEVUELVE EL NOMBRE DEL ULTIMO PROFESOR POR ID REGISTRADO EN LA BD

    public static void count_Teacher_By_Department_Procedure() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();
            String sql = "DROP PROCEDURE count_Teacher_By_Department_Procedure IF EXISTS;";
            stt.executeUpdate(sql);

            sql = "CREATE PROCEDURE count_Teacher_By_Department_Procedure(IN id int, OUT name Varchar(20), OUT numero int)"
                    + "READS sql DATA "
                    + "BEGIN ATOMIC "
                    + "SET numero = (SELECT COUNT(*) id FROM teachers WHERE dept_num IN (SELECT dept_num FROM departments WHERE name=name)); " // pasar al set el parametro out
                    // SELECT COUNT(*)as numero  FROM teachers WHERE dept_num IN (SELECT dept_num FROM departments WHERE name = 'informatica');
                    + "END";
            stt.executeUpdate(sql);
            con.close();
        } // 
        catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    } // PROCEDIMIENTO (IN & OUT) PARA BUSCAR PROFESORES POR DEPARTAMENTO

    public static void count_Teacher_By_Department() {
        try {
            Connection con = conexion("jdbc:hsqldb:.\\database\\db");
            Statement stt;
            stt = con.createStatement();
            CallableStatement call = con.prepareCall(" call count_Teacher_By_Department_Procedure(?,?,?)");
            call.setInt(1, 1);
            call.registerOutParameter(2, Types.VARCHAR); // pasar el primer parametro out y definir su tipo
            call.registerOutParameter(3, Types.INTEGER);
            call.execute();
            System.out.println("El total de profesores es: " +call.getString(3));
            con.close();
            
        } // VACIO
        catch (SQLException ex) {
            Logger.getLogger(Act3_4.class.getName()).log(Level.SEVERE, null, ex);
        }
    }  // BUSCA PROFESORES POR DEPARTAMENTO (PERO ME SACA EL TOTAL DE TODOS LOS PROFESORES NO FILTRA DEPARTAMENTO)

    public static void exit() {
        Scanner tcl = new Scanner(System.in);
        String yes = "";
        System.out.println("¿De verdad quieres salir del programa?\nsi o no");
        yes = tcl.nextLine();
        if (yes.equalsIgnoreCase("si")) {
            System.out.println("Saliendo del programa!!!");
        } else {
            menu();
        }
    } // TE PREGUNTA SI QUIERES SALIR DEL PROGRAMA ( SI ES NO, TE MUESTRA EL MENU() )

    public static void menu() {
        Scanner tcl = new Scanner(System.in);
        int op = 0;
        String bienvenida = ("BIENVENIDO AL EJERCICIO MÁS LARGO DE LA HISTORIA!!!\n"
                + "1. Set new department\n2. Set new teacher\n3. Set salary to...\n4. Rise salary a %"
                + "\n5. Rise salary a % to a department like...\n6. Get newest teacher name"
                + "\n7. Count teacher of department...\n8. Do query to the database\n9. Quit");

        while (op != 9) {
            System.out.println(bienvenida);

            op = tcl.nextInt();
            switch (op) {
                case 1:
                    newDepartment();
                    break;
                case 2:
                    newTeacher();
                    break;
                case 3:
                    set_Salary();
                    break;
                case 4:
                    salary_Porcentaje();
                    break;
                case 5:
                    salary_Porcentaje_Department();
                    break;
                case 6:
                    get_Newest_Teacher2();
                    break;
                case 7:
                    count_Teacher_By_Department();
                    break;
                case 8:
                    consultaDB();
                    break;
                case 9:
                    exit();
                    break;
                default:
                    menu();
                    break;
            }
        }
    } // MENU CON UN SWITCH 

}// FIN DE LA CLASE
