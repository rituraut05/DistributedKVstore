/*
 * Taken from: https://gist.github.com/silversthem/448a61241a34c97ed9aedd1b84187d8e
 * This timer handles ticks, ticks are a predetermined duration, usually in msecs.
 */

#ifndef TIMER_HPP
#define TIMER_HPP

#include <chrono>

namespace util {
  class Timer {
    protected:
      int _tick_duration; // Tick duration in msec
      int  _current_tick; // Current tick
      bool       _update; // If the tick count has updated
      bool      _running; // To indicate if timer is running
      /* Timers */
      std::chrono::time_point<std::chrono::system_clock> _start;   // Time start
      std::chrono::time_point<std::chrono::system_clock>   _end;   // End
      std::chrono::duration<double> _elapsed;                      // Delta
      /* Calculations */
      void calculate(); // Calculates the stuff
    public:
      int       _timeout; // Timeout at which to trigger other things
      Timer(); // Default constructor
      Timer(int const& tick_duration, int timeout); // Creates a timer
      /* Getters */
      int get_tick(); // Returns current tick
      /* Setters */
      void set_tick_duration(int const& d); // Sets tick duration
      void       set_running(bool running); // Sets running 
      /* Methods */
      void start(int timeout);   // Starts the timer
      void reset(int timeout);   // Resets the timer
      bool updated(); // If the tick count has changed
      bool running(); // If the timer is running or not   
  };
}

#endif