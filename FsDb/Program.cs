using System;
using System.Linq.Expressions;

namespace FsDb
{
    class Program
    {
        public static void Main()
        {
            var x = Expression.Parameter(typeof(int), "x");
            var y = Expression.Parameter(typeof(int), "y");
           
            while (true)
            {
                Console.WriteLine("f (x, y) = ");
                var exp = Console.ReadLine();
                
                try
                {
                    var expression = System.Linq.Dynamic.DynamicExpression.ParseLambda(new[] { x, y }, null, exp);
                    var compiled = expression.Compile();
                    for (int _x = 1; _x <= 3; ++_x)
                        for (int _y = 1; _y <= 3; ++_y)
                            Console.WriteLine("f({0},{1}) = {2}", _x, _y, compiled.DynamicInvoke(_x, _y));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
    }
}
